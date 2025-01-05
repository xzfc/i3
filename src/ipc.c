/*
 * vim:ts=4:sw=4:expandtab
 *
 * i3 - an improved tiling window manager
 * © 2009 Michael Stapelberg and contributors (see also: LICENSE)
 *
 * ipc.c: UNIX domain socket IPC (initialization, client handling, protocol).
 *
 */

#include "all.h"
#include "yajl_utils.h"

#include <ev.h>
#include <fcntl.h>
#include <libgen.h>
#include <locale.h>
#include <stdint.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

#include <yajl/yajl_gen.h>
#include <yajl/yajl_parse.h>
#include <json-c/json_object.h>

char *current_socketpath = NULL;

TAILQ_HEAD(ipc_client_head, ipc_client) all_clients = TAILQ_HEAD_INITIALIZER(all_clients);

static void ipc_client_timeout(EV_P_ ev_timer *w, int revents);
static void ipc_socket_writeable_cb(EV_P_ struct ev_io *w, int revents);

static ev_tstamp kill_timeout = 10.0;

void ipc_set_kill_timeout(ev_tstamp new) {
    kill_timeout = new;
}

/*
 * Try to write the contents of the pending buffer to the client's subscription
 * socket. Will set, reset or clear the timeout and io write callbacks depending
 * on the result of the write operation.
 *
 */
static void ipc_push_pending(ipc_client *client) {
    const ssize_t result = writeall_nonblock(client->fd, client->buffer, client->buffer_size);
    if (result < 0) {
        return;
    }

    if ((size_t)result == client->buffer_size) {
        /* Everything was written successfully: clear the timer and stop the io
         * callback. */
        FREE(client->buffer);
        client->buffer_size = 0;
        if (client->timeout) {
            ev_timer_stop(main_loop, client->timeout);
            FREE(client->timeout);
        }
        ev_io_stop(main_loop, client->write_callback);
        return;
    }

    /* Otherwise, make sure that the io callback is enabled and create a new
     * timer if needed. */
    ev_io_start(main_loop, client->write_callback);

    if (!client->timeout) {
        struct ev_timer *timeout = scalloc(1, sizeof(struct ev_timer));
        ev_timer_init(timeout, ipc_client_timeout, kill_timeout, 0.);
        timeout->data = client;
        client->timeout = timeout;
        ev_set_priority(timeout, EV_MINPRI);
        ev_timer_start(main_loop, client->timeout);
    } else if (result > 0) {
        /* Keep the old timeout when nothing is written. Otherwise, we would
         * keep a dead connection by continuously renewing its timeouts. */
        ev_timer_stop(main_loop, client->timeout);
        ev_timer_set(client->timeout, kill_timeout, 0.0);
        ev_timer_start(main_loop, client->timeout);
    }
    if (result == 0) {
        return;
    }

    /* Shift the buffer to the left and reduce the allocated space. */
    client->buffer_size -= (size_t)result;
    memmove(client->buffer, client->buffer + result, client->buffer_size);
    client->buffer = srealloc(client->buffer, client->buffer_size);
}

/*
 * Given a message and a message type, create the corresponding header, merge it
 * with the message and append it to the given client's output buffer. Also,
 * send the message if the client's buffer was empty.
 *
 */
static void ipc_send_client_message_raw(ipc_client *client, size_t size, const uint32_t message_type, const uint8_t *payload) {
    const i3_ipc_header_t header = {
        .magic = {'i', '3', '-', 'i', 'p', 'c'},
        .size = size,
        .type = message_type};
    const size_t header_size = sizeof(i3_ipc_header_t);
    const size_t message_size = header_size + size;

    const bool push_now = (client->buffer_size == 0);
    client->buffer = srealloc(client->buffer, client->buffer_size + message_size);
    memcpy(client->buffer + client->buffer_size, ((void *)&header), header_size);
    memcpy(client->buffer + client->buffer_size + header_size, payload, size);
    client->buffer_size += message_size;

    if (push_now) {
        ipc_push_pending(client);
    }
}

/*
 * Similar to ipc_send_client_message_raw, but takes a json_object and converts it to a string.
 *
 * Takes ownership of the json_object.
 */
static void ipc_send_client_message(ipc_client *client, const uint32_t message_type, json_object *obj) {
    size_t size;
    const char *payload = json_object_to_json_string_length(obj, JSON_C_TO_STRING_PLAIN, &size);
    ipc_send_client_message_raw(client, size, message_type, (const uint8_t *)payload);
    json_object_put(obj);
}

static void free_ipc_client(ipc_client *client, int exempt_fd) {
    if (client->fd != exempt_fd) {
        DLOG("Disconnecting client on fd %d\n", client->fd);
        close(client->fd);
    }

    ev_io_stop(main_loop, client->read_callback);
    FREE(client->read_callback);
    ev_io_stop(main_loop, client->write_callback);
    FREE(client->write_callback);
    if (client->timeout) {
        ev_timer_stop(main_loop, client->timeout);
        FREE(client->timeout);
    }

    free(client->buffer);

    for (int i = 0; i < client->num_events; i++) {
        free(client->events[i]);
    }
    free(client->events);
    TAILQ_REMOVE(&all_clients, client, clients);
    free(client);
}

/*
 * Sends the specified event to all IPC clients which are currently connected
 * and subscribed to this kind of event.
 *
 */
void ipc_send_event_raw(const char *event, uint32_t message_type, const char *payload, size_t size) {
    ipc_client *current;
    TAILQ_FOREACH (current, &all_clients, clients) {
        for (int i = 0; i < current->num_events; i++) {
            if (strcasecmp(current->events[i], event) == 0) {
                ipc_send_client_message_raw(current, size, message_type, (const uint8_t *)payload);
                break;
            }
        }
    }
}

/*
 * Sends the specified event to all IPC clients which are currently connected
 * and subscribed to this kind of event.
 *
 * Takes ownership of the json_object.
 */
void ipc_send_event(const char *event, uint32_t message_type, json_object *obj) {
    size_t size;
    const char *payload = json_object_to_json_string_length(obj, JSON_C_TO_STRING_PLAIN, &size);
    ipc_send_event_raw(event, message_type, payload, size);
    json_object_put(obj);
}

/*
 * For shutdown events, we send the reason for the shutdown.
 */
static void ipc_send_shutdown_event(shutdown_reason_t reason) {
    json_object *obj = json_object_new_object();

    if (reason == SHUTDOWN_REASON_RESTART) {
        json_object_object_add(obj, "change", json_object_new_string("restart"));
    } else if (reason == SHUTDOWN_REASON_EXIT) {
        json_object_object_add(obj, "change", json_object_new_string("exit"));
    }

    ipc_send_event("shutdown", I3_IPC_EVENT_SHUTDOWN, obj);
}

/*
 * Calls shutdown() on each socket and closes it. This function is to be called
 * when exiting or restarting only!
 *
 * exempt_fd is never closed. Set to -1 to close all fds.
 *
 */
void ipc_shutdown(shutdown_reason_t reason, int exempt_fd) {
    ipc_send_shutdown_event(reason);

    ipc_client *current;
    while (!TAILQ_EMPTY(&all_clients)) {
        current = TAILQ_FIRST(&all_clients);
        if (current->fd != exempt_fd) {
            shutdown(current->fd, SHUT_RDWR);
        }
        free_ipc_client(current, exempt_fd);
    }
}

/*
 * Executes the given command.
 *
 */
IPC_HANDLER(run_command) {
    /* To get a properly terminated buffer, we copy
     * message_size bytes out of the buffer */
    char *command = sstrndup((const char *)message, message_size);
    LOG("IPC: received: *%.4000s*\n", command);
    yajl_gen gen = yajl_gen_alloc(NULL);

    CommandResult *result = parse_command(command, gen, client);
    free(command);

    if (result->needs_tree_render) {
        tree_render();
    }

    command_result_free(result);

    const unsigned char *reply;
    ylength length;
    yajl_gen_get_buf(gen, &reply, &length);

    ipc_send_client_message_raw(client, length, I3_IPC_REPLY_TYPE_COMMAND,
                                (const uint8_t *)reply);

    yajl_gen_free(gen);
}

static json_object *dump_success(bool success) {
    json_object *obj = json_object_new_object();
    json_object_object_add(obj, "success", json_object_new_boolean(success));
    return obj;
}

static void dump_rect_yajl(yajl_gen gen, const char *name, Rect r) {
    ystr(name);
    y(map_open);
    ystr("x");
    y(integer, (int32_t)r.x);
    ystr("y");
    y(integer, (int32_t)r.y);
    ystr("width");
    y(integer, r.width);
    ystr("height");
    y(integer, r.height);
    y(map_close);
}

static json_object *dump_rect(Rect r) {
    json_object *obj = json_object_new_object();
    json_object_object_add(obj, "x", json_object_new_int64(r.x));
    json_object_object_add(obj, "y", json_object_new_int64(r.y));
    json_object_object_add(obj, "width", json_object_new_int64(r.width));
    json_object_object_add(obj, "height", json_object_new_int64(r.height));
    return obj;
}

static json_object *dump_gaps(gaps_t gaps) {
    json_object *obj = json_object_new_object();
    json_object_object_add(obj, "inner", json_object_new_int64(gaps.inner));

    // TODO: the i3ipc Python modules recognize gaps, but only inner/outer
    // This is currently here to preserve compatibility with that
    json_object_object_add(obj, "outer", json_object_new_int64(gaps.top));

    json_object_object_add(obj, "top", json_object_new_int64(gaps.top));
    json_object_object_add(obj, "right", json_object_new_int64(gaps.right));
    json_object_object_add(obj, "bottom", json_object_new_int64(gaps.bottom));
    json_object_object_add(obj, "left", json_object_new_int64(gaps.left));

    return obj;
}

static json_object *dump_event_state_mask(Binding *bind) {
    json_object *obj = json_object_new_array();
    for (int i = 0; i < 20; i++) {
        if (bind->event_state_mask & (1 << i)) {
            const char *name = NULL;
            switch (1 << i) {
                case XCB_KEY_BUT_MASK_SHIFT:
                    name = "shift";
                    break;
                case XCB_KEY_BUT_MASK_LOCK:
                    name = "lock";
                    break;
                case XCB_KEY_BUT_MASK_CONTROL:
                    name = "ctrl";
                    break;
                case XCB_KEY_BUT_MASK_MOD_1:
                    name = "Mod1";
                    break;
                case XCB_KEY_BUT_MASK_MOD_2:
                    name = "Mod2";
                    break;
                case XCB_KEY_BUT_MASK_MOD_3:
                    name = "Mod3";
                    break;
                case XCB_KEY_BUT_MASK_MOD_4:
                    name = "Mod4";
                    break;
                case XCB_KEY_BUT_MASK_MOD_5:
                    name = "Mod5";
                    break;
                case XCB_KEY_BUT_MASK_BUTTON_1:
                    name = "Button1";
                    break;
                case XCB_KEY_BUT_MASK_BUTTON_2:
                    name = "Button2";
                    break;
                case XCB_KEY_BUT_MASK_BUTTON_3:
                    name = "Button3";
                    break;
                case XCB_KEY_BUT_MASK_BUTTON_4:
                    name = "Button4";
                    break;
                case XCB_KEY_BUT_MASK_BUTTON_5:
                    name = "Button5";
                    break;
                case (I3_XKB_GROUP_MASK_1 << 16):
                    name = "Group1";
                    break;
                case (I3_XKB_GROUP_MASK_2 << 16):
                    name = "Group2";
                    break;
                case (I3_XKB_GROUP_MASK_3 << 16):
                    name = "Group3";
                    break;
                case (I3_XKB_GROUP_MASK_4 << 16):
                    name = "Group4";
                    break;
            }
            if (name) {
                json_object_array_add(obj, json_object_new_string(name));
            }
        }
    }
    return obj;
}

static json_object *dump_binding(Binding *bind) {
    json_object *obj = json_object_new_object();
    json_object_object_add(obj, "input_code", json_object_new_int64(bind->keycode));
    json_object_object_add(obj, "input_type",
                           json_object_new_string(bind->input_type == B_KEYBOARD ? "keyboard" : "mouse"));
    json_object_object_add(obj, "symbol",
                           bind->symbol ? json_object_new_string(bind->symbol) : NULL);
    json_object_object_add(obj, "event_state_mask", dump_event_state_mask(bind));
    json_object_object_add(obj, "command", json_object_new_string(bind->command));
    // This key is only provided for compatibility, new programs should use
    // event_state_mask instead.
    json_object_object_add(obj, "mods",
                           dump_event_state_mask(bind));
    return obj;
}

json_object *dump_node(struct Con *con, bool inplace_restart) {
    json_object *obj = json_object_new_object();
    json_object_object_add(obj, "id", json_object_new_uint64((uintptr_t)con));

    static const char *CON_TYPE_NAMES[] = {
        [CT_ROOT] = "root",
        [CT_OUTPUT] = "output",
        [CT_CON] = "con",
        [CT_FLOATING_CON] = "floating_con",
        [CT_WORKSPACE] = "workspace",
        [CT_DOCKAREA] = "dockarea",
    };
    json_object_object_add(obj, "type", json_object_new_string(CON_TYPE_NAMES[con->type]));

    /* provided for backwards compatibility only. */
    const char *orientation_name = NULL;
    if (!con_is_split(con)) {
        orientation_name = "none";
    } else {
        if (con_orientation(con) == HORIZ) {
            orientation_name = "horizontal";
        } else {
            orientation_name = "vertical";
        }
    }
    json_object_object_add(obj, "orientation", json_object_new_string(orientation_name));

    static const char *SCRATCHPAD_STATE_NAMES[] = {
        [SCRATCHPAD_NONE] = "none",
        [SCRATCHPAD_FRESH] = "fresh",
        [SCRATCHPAD_CHANGED] = "changed",
    };
    json_object_object_add(obj, "scratchpad_state",
                           json_object_new_string(SCRATCHPAD_STATE_NAMES[con->scratchpad_state]));

    json_object_object_add(obj, "percent",
                           con->percent == 0.0 ? NULL : json_object_new_double(con->percent));

    json_object_object_add(obj, "urgent", json_object_new_boolean(con->urgent));

    json_object *marks = json_object_new_array();
    mark_t *mark;
    TAILQ_FOREACH (mark, &(con->marks_head), marks) {
        json_object_array_add(marks, json_object_new_string(mark->name));
    }
    json_object_object_add(obj, "marks", marks);

    json_object_object_add(obj, "focused", json_object_new_boolean(con == focused));

    if (con->type != CT_ROOT && con->type != CT_OUTPUT) {
        json_object_object_add(obj, "output", json_object_new_string(con_get_output(con)->name));
    }

    if (con->layout == L_DEFAULT) {
        DLOG("About to dump layout=default, this is a bug in the code.\n");
        assert(false);
    }
    static const char *LAYOUT_NAMES[] = {
        [L_DEFAULT] = "default",
        [L_STACKED] = "stacked",
        [L_TABBED] = "tabbed",
        [L_DOCKAREA] = "dockarea",
        [L_OUTPUT] = "output",
        [L_SPLITV] = "splitv",
        [L_SPLITH] = "splith",
    };
    json_object_object_add(obj, "layout", json_object_new_string(LAYOUT_NAMES[con->layout]));

    switch (con->workspace_layout) {
        case L_DEFAULT:
        case L_STACKED:
        case L_TABBED:
            json_object_object_add(obj, "workspace_layout",
                                   json_object_new_string(LAYOUT_NAMES[con->workspace_layout]));
            break;
        default:
            DLOG("About to dump workspace_layout=%d (none of default/stacked/tabbed), this is a bug.\n", con->workspace_layout);
            assert(false);
            break;
    }

    json_object_object_add(obj, "last_split_layout",
                           json_object_new_string(LAYOUT_NAMES[con->layout]));

    static const char *BORDER_STYLE_NAMES[] = {
        [BS_NORMAL] = "normal",
        [BS_NONE] = "none",
        [BS_PIXEL] = "pixel",
    };
    json_object_object_add(obj, "border",
                           json_object_new_string(BORDER_STYLE_NAMES[con->border_style]));

    json_object_object_add(obj, "current_border_width",
                           json_object_new_int64(con->current_border_width));

    json_object_object_add(obj, "rect", dump_rect(con->rect));
    if (con_draw_decoration_into_frame(con)) {
        Rect simulated_deco_rect = con->deco_rect;
        simulated_deco_rect.x = con->rect.x - con->parent->rect.x;
        simulated_deco_rect.y = con->rect.y - con->parent->rect.y;
        json_object_object_add(obj, "deco_rect", dump_rect(simulated_deco_rect));
        json_object_object_add(obj, "actual_deco_rect", dump_rect(con->deco_rect));
    } else {
        json_object_object_add(obj, "deco_rect", dump_rect(con->deco_rect));
    }
    json_object_object_add(obj, "window_rect", dump_rect(con->window_rect));
    json_object_object_add(obj, "geometry", dump_rect(con->geometry));

    if (con->window && con->window->name) {
        json_object_object_add(obj, "name",
                               json_object_new_string(i3string_as_utf8(con->window->name)));
    } else if (con->name != NULL) {
        json_object_object_add(obj, "name", json_object_new_string(con->name));
    } else {
        json_object_object_add(obj, "name", NULL);
    }

    if (con->title_format != NULL) {
        json_object_object_add(obj, "title_format", json_object_new_string(con->title_format));
    }

    json_object_object_add(obj, "window_icon_padding", json_object_new_int64(con->window_icon_padding));

    if (con->type == CT_WORKSPACE) {
        json_object_object_add(obj, "num", json_object_new_int64(con->num));
        json_object_object_add(obj, "gaps", dump_gaps(con->gaps));
    }

    json_object_object_add(obj, "window",
                           con->window ? json_object_new_int64(con->window->id) : NULL);

    const char *window_type_name = NULL;
    if (con->window) {
        if (con->window->window_type == A__NET_WM_WINDOW_TYPE_NORMAL) {
            window_type_name = "normal";
        } else if (con->window->window_type == A__NET_WM_WINDOW_TYPE_DOCK) {
            window_type_name = "dock";
        } else if (con->window->window_type == A__NET_WM_WINDOW_TYPE_DIALOG) {
            window_type_name = "dialog";
        } else if (con->window->window_type == A__NET_WM_WINDOW_TYPE_UTILITY) {
            window_type_name = "utility";
        } else if (con->window->window_type == A__NET_WM_WINDOW_TYPE_TOOLBAR) {
            window_type_name = "toolbar";
        } else if (con->window->window_type == A__NET_WM_WINDOW_TYPE_SPLASH) {
            window_type_name = "splash";
        } else if (con->window->window_type == A__NET_WM_WINDOW_TYPE_MENU) {
            window_type_name = "menu";
        } else if (con->window->window_type == A__NET_WM_WINDOW_TYPE_DROPDOWN_MENU) {
            window_type_name = "dropdown_menu";
        } else if (con->window->window_type == A__NET_WM_WINDOW_TYPE_POPUP_MENU) {
            window_type_name = "popup_menu";
        } else if (con->window->window_type == A__NET_WM_WINDOW_TYPE_TOOLTIP) {
            window_type_name = "tooltip";
        } else if (con->window->window_type == A__NET_WM_WINDOW_TYPE_NOTIFICATION) {
            window_type_name = "notification";
        } else {
            window_type_name = "unknown";
        }
    }
    json_object_object_add(obj, "window_type",
                           window_type_name ? json_object_new_string(window_type_name) : NULL);

    if (con->window && !inplace_restart) {
        /* Window properties are useless to preserve when restarting because
         * they will be queried again anyway. However, for i3-save-tree(1),
         * they are very useful and save i3-save-tree dealing with X11. */
        json_object *window_properties = json_object_new_object();
        json_object_object_add(obj, "window_properties", window_properties);

#define DUMP_PROPERTY(key, prop_name)                            \
    do {                                                         \
        if (con->window->prop_name != NULL) {                    \
            json_object_object_add(                              \
                window_properties, key,                          \
                json_object_new_string(con->window->prop_name)); \
        }                                                        \
    } while (0)

        DUMP_PROPERTY("class", class_class);
        DUMP_PROPERTY("instance", class_instance);
        DUMP_PROPERTY("window_role", role);
        DUMP_PROPERTY("machine", machine);

        if (con->window->name != NULL) {
            json_object_object_add(
                window_properties, "title",
                json_object_new_string(i3string_as_utf8(con->window->name)));
        }

        if (con->window->transient_for == XCB_NONE) {
            json_object_object_add(window_properties, "transient_for", NULL);
        } else {
            json_object_object_add(window_properties, "transient_for",
                                   json_object_new_int64(con->window->transient_for));
        }
    }

    json_object *nodes = json_object_new_array();
    json_object_object_add(obj, "nodes", nodes);
    Con *node;
    if (con->type != CT_DOCKAREA || !inplace_restart) {
        TAILQ_FOREACH (node, &(con->nodes_head), nodes) {
            json_object_array_add(nodes, dump_node(node, inplace_restart));
        }
    }

    json_object *floating_nodes = json_object_new_array();
    json_object_object_add(obj, "floating_nodes", floating_nodes);
    TAILQ_FOREACH (node, &(con->floating_head), floating_windows) {
        json_object_array_add(floating_nodes, dump_node(node, inplace_restart));
    }

    json_object *focus = json_object_new_array();
    json_object_object_add(obj, "focus", focus);
    TAILQ_FOREACH (node, &(con->focus_head), focused) {
        json_object_array_add(focus, json_object_new_uint64((uintptr_t)node));
    }

    json_object_object_add(obj, "fullscreen_mode", json_object_new_int(con->fullscreen_mode));

    json_object_object_add(obj, "sticky", json_object_new_boolean(con->sticky));

    static const char *FLOATING_NAMES[] = {
        [FLOATING_AUTO_OFF] = "auto_off",
        [FLOATING_AUTO_ON] = "auto_on",
        [FLOATING_USER_OFF] = "user_off",
        [FLOATING_USER_ON] = "user_on",
    };
    json_object_object_add(obj, "floating", json_object_new_string(FLOATING_NAMES[con->floating]));

    json_object *swallows = json_object_new_array();
    json_object_object_add(obj, "swallows", swallows);
    Match *match;
    TAILQ_FOREACH (match, &(con->swallow_head), matches) {
        /* We will generate a new restart_mode match specification after this
         * loop, so skip this one. */
        if (match->restart_mode) {
            continue;
        }
        json_object *swallow = json_object_new_object();
        json_object_array_add(swallows, swallow);
        if (match->dock != M_DONTCHECK) {
            json_object_object_add(swallow, "dock", json_object_new_int(match->dock));
            json_object_object_add(swallow, "insert_where",
                                   json_object_new_int(match->insert_where));
        }

#define DUMP_REGEX(re_name)                                       \
    do {                                                          \
        if (match->re_name != NULL) {                             \
            json_object_object_add(                               \
                swallow, #re_name,                                \
                json_object_new_string(match->re_name->pattern)); \
        }                                                         \
    } while (0)

        DUMP_REGEX(class);
        DUMP_REGEX(instance);
        DUMP_REGEX(window_role);
        DUMP_REGEX(title);
        DUMP_REGEX(machine);

#undef DUMP_REGEX
    }

    if (inplace_restart) {
        if (con->window != NULL) {
            json_object *swallow = json_object_new_object();
            json_object_array_add(swallows, swallow);
            json_object_object_add(swallow, "id", json_object_new_int64(con->window->id));
            json_object_object_add(swallow, "restart_mode", json_object_new_boolean(true));
        }
    }

    if (inplace_restart && con->window != NULL) {
        json_object_object_add(obj, "depth", json_object_new_int(con->depth));
    }

    if (inplace_restart && con->type == CT_ROOT && previous_workspace_name) {
        json_object_object_add(obj, "previous_workspace_name", json_object_new_string(previous_workspace_name));
    }

    return obj;
}

static json_object *dump_bar_bindings(Barconfig *config) {
    if (TAILQ_EMPTY(&(config->bar_bindings))) {
        return NULL;
    }

    json_object *bindings = json_object_new_array();

    struct Barbinding *current;
    TAILQ_FOREACH (current, &(config->bar_bindings), bindings) {
        json_object *binding = json_object_new_object();
        json_object_array_add(bindings, binding);

        json_object_object_add(binding, "input_code",
                               json_object_new_int64(current->input_code));
        json_object_object_add(binding, "command",
                               json_object_new_string(current->command));
        json_object_object_add(binding, "release",
                               json_object_new_boolean(current->release == B_UPON_KEYRELEASE));
    }

    return bindings;
}

static char *canonicalize_output_name(char *name) {
    /* Do not canonicalize special output names. */
    if (strcasecmp(name, "primary") == 0 || strcasecmp(name, "nonprimary") == 0) {
        return name;
    }
    Output *output = get_output_by_name(name, false);
    return output ? output_primary_name(output) : name;
}

static json_object *dump_bar_config(Barconfig *config) {
    json_object *obj = json_object_new_object();
    json_object_object_add(obj, "id", json_object_new_string(config->id));

    if (config->num_outputs > 0) {
        json_object *outputs = json_object_new_array();
        json_object_object_add(obj, "outputs", outputs);
        for (int c = 0; c < config->num_outputs; c++) {
            /* Convert monitor names (RandR ≥ 1.5) or output names
             * (RandR < 1.5) into monitor names. This way, existing
             * configs which use output names transparently keep
             * working. */
            json_object_array_add(outputs,
                                  json_object_new_string(canonicalize_output_name(config->outputs[c])));
        }
    }

    if (!TAILQ_EMPTY(&(config->tray_outputs))) {
        json_object *tray_outputs = json_object_new_array();
        json_object_object_add(obj, "tray_outputs", tray_outputs);

        struct tray_output_t *tray_output;
        TAILQ_FOREACH (tray_output, &(config->tray_outputs), tray_outputs) {
            json_object_array_add(tray_outputs,
                                  json_object_new_string(canonicalize_output_name(tray_output->output)));
        }
    }

#define YSTR_IF_SET(name)                              \
    do {                                               \
        if (config->name) {                            \
            json_object_object_add(                    \
                obj, #name,                            \
                json_object_new_string(config->name)); \
        }                                              \
    } while (0)

    json_object_object_add(obj, "tray_padding",
                           json_object_new_int64(config->tray_padding));

    YSTR_IF_SET(socket_path);

    static const char *MODE_NAMES[] = {
        [M_HIDE] = "hide",
        [M_INVISIBLE] = "invisible",
        [M_DOCK] = "dock",
    };
    json_object_object_add(obj, "mode", json_object_new_string(MODE_NAMES[config->mode]));

    static const char *HIDDEN_STATE_NAMES[] = {
        [S_HIDE] = "hide",
        [S_SHOW] = "show",
    };
    json_object_object_add(obj, "hidden_state",
                           json_object_new_string(HIDDEN_STATE_NAMES[config->hidden_state]));

    json_object_object_add(obj, "modifier", json_object_new_int64(config->modifier));

    json_object *bindings = dump_bar_bindings(config);
    if (bindings) {
        json_object_object_add(obj, "bindings", bindings);
    }

    static const char *POSITION_NAMES[] = {
        [P_BOTTOM] = "bottom",
        [P_TOP] = "top",
    };
    json_object_object_add(obj, "position",
                           json_object_new_string(POSITION_NAMES[config->position]));

    YSTR_IF_SET(status_command);
    YSTR_IF_SET(workspace_command);
    YSTR_IF_SET(font);

    if (config->bar_height) {
        json_object_object_add(obj, "bar_height",
                               json_object_new_int64(config->bar_height));
    }

    json_object_object_add(obj, "padding", dump_rect(config->padding));

    YSTR_IF_SET(separator_symbol);

    json_object_object_add(obj, "workspace_buttons",
                           json_object_new_boolean(!config->hide_workspace_buttons));
    json_object_object_add(obj, "workspace_min_width",
                           json_object_new_int64(config->workspace_min_width));
    json_object_object_add(obj, "strip_workspace_numbers",
                           json_object_new_boolean(config->strip_workspace_numbers));
    json_object_object_add(obj, "strip_workspace_name",
                           json_object_new_boolean(config->strip_workspace_name));
    json_object_object_add(obj, "binding_mode_indicator",
                           json_object_new_boolean(!config->hide_binding_mode_indicator));
    json_object_object_add(obj, "verbose", json_object_new_boolean(config->verbose));

#undef YSTR_IF_SET
#define YSTR_IF_SET(name)                                     \
    do {                                                      \
        if (config->colors.name) {                            \
            json_object_object_add(                           \
                colors, #name,                                \
                json_object_new_string(config->colors.name)); \
        }                                                     \
    } while (0)

    json_object *colors = json_object_new_object();
    json_object_object_add(obj, "colors", colors);
    YSTR_IF_SET(background);
    YSTR_IF_SET(statusline);
    YSTR_IF_SET(separator);
    YSTR_IF_SET(focused_background);
    YSTR_IF_SET(focused_statusline);
    YSTR_IF_SET(focused_separator);
    YSTR_IF_SET(focused_workspace_border);
    YSTR_IF_SET(focused_workspace_bg);
    YSTR_IF_SET(focused_workspace_text);
    YSTR_IF_SET(active_workspace_border);
    YSTR_IF_SET(active_workspace_bg);
    YSTR_IF_SET(active_workspace_text);
    YSTR_IF_SET(inactive_workspace_border);
    YSTR_IF_SET(inactive_workspace_bg);
    YSTR_IF_SET(inactive_workspace_text);
    YSTR_IF_SET(urgent_workspace_border);
    YSTR_IF_SET(urgent_workspace_bg);
    YSTR_IF_SET(urgent_workspace_text);
    YSTR_IF_SET(binding_mode_border);
    YSTR_IF_SET(binding_mode_bg);
    YSTR_IF_SET(binding_mode_text);

#undef YSTR_IF_SET
    return obj;
}

IPC_HANDLER(tree) {
    ipc_send_client_message(client, I3_IPC_REPLY_TYPE_TREE, dump_node(croot, false));
}

/*
 * Formats the reply message for a GET_WORKSPACES request and sends it to the
 * client
 *
 */
IPC_HANDLER(get_workspaces) {
    json_object *obj = json_object_new_array();

    Con *focused_ws = con_get_workspace(focused);

    Con *output;
    TAILQ_FOREACH (output, &(croot->nodes_head), nodes) {
        if (con_is_internal(output)) {
            continue;
        }
        Con *ws;
        TAILQ_FOREACH (ws, &(output_get_content(output)->nodes_head), nodes) {
            assert(ws->type == CT_WORKSPACE);

            json_object *ws_obj = json_object_new_object();
            json_object_array_add(obj, ws_obj);

            json_object_object_add(ws_obj, "id", json_object_new_int64((uintptr_t)ws));
            json_object_object_add(ws_obj, "num", json_object_new_int64(ws->num));
            json_object_object_add(ws_obj, "name", json_object_new_string(ws->name));
            json_object_object_add(ws_obj, "visible", json_object_new_boolean(workspace_is_visible(ws)));
            json_object_object_add(ws_obj, "focused", json_object_new_boolean(ws == focused_ws));
            json_object_object_add(ws_obj, "rect", dump_rect(ws->rect));
            json_object_object_add(ws_obj, "output", json_object_new_string(output->name));
            json_object_object_add(ws_obj, "urgent", json_object_new_boolean(ws->urgent));
        }
    }

    ipc_send_client_message(client, I3_IPC_REPLY_TYPE_WORKSPACES, obj);
}

/*
 * Formats the reply message for a GET_OUTPUTS request and sends it to the
 * client
 *
 */
IPC_HANDLER(get_outputs) {
    json_object *obj = json_object_new_array();

    Output *output;
    TAILQ_FOREACH (output, &outputs, outputs) {
        json_object *elem = json_object_new_object();
        json_object_array_add(obj, elem);

        json_object_object_add(elem, "name", json_object_new_string(output_primary_name(output)));
        json_object_object_add(elem, "active", json_object_new_boolean(output->active));
        json_object_object_add(elem, "primary", json_object_new_boolean(output->primary));
        json_object_object_add(elem, "rect", dump_rect(output->rect));

        Con *ws = NULL;
        if (output->con && (ws = con_get_fullscreen_con(output->con, CF_OUTPUT))) {
            json_object_object_add(elem, "current_workspace", json_object_new_string(ws->name));
        } else {
            json_object_object_add(elem, "current_workspace", NULL);
        }
    }

    ipc_send_client_message(client, I3_IPC_REPLY_TYPE_OUTPUTS, obj);
}

/*
 * Formats the reply message for a GET_MARKS request and sends it to the
 * client
 *
 */
IPC_HANDLER(get_marks) {
    json_object *obj = json_object_new_array();

    Con *con;
    TAILQ_FOREACH (con, &all_cons, all_cons) {
        mark_t *mark;
        TAILQ_FOREACH (mark, &(con->marks_head), marks) {
            json_object_array_add(obj, json_object_new_string(mark->name));
        }
    }

    ipc_send_client_message(client, I3_IPC_REPLY_TYPE_MARKS, obj);
}

/*
 * Returns the version of i3
 *
 */
IPC_HANDLER(get_version) {
    json_object *obj = json_object_new_object();
    json_object_object_add(obj, "major", json_object_new_int(MAJOR_VERSION));
    json_object_object_add(obj, "minor", json_object_new_int(MINOR_VERSION));
    json_object_object_add(obj, "patch", json_object_new_int(PATCH_VERSION));
    json_object_object_add(obj, "human_readable", json_object_new_string(i3_version));
    json_object_object_add(obj, "loaded_config_file_name", json_object_new_string(current_configpath));

    json_object *fnames = json_object_new_array();
    json_object_object_add(obj, "included_config_file_names", fnames);
    IncludedFile *file;
    TAILQ_FOREACH (file, &included_files, files) {
        if (file == TAILQ_FIRST(&included_files)) {
            /* Skip the first file, which is current_configpath. */
            continue;
        }
        json_object_array_add(fnames, json_object_new_string(file->path));
    }

    ipc_send_client_message(client, I3_IPC_REPLY_TYPE_VERSION, obj);
}

/*
 * Formats the reply message for a GET_BAR_CONFIG request and sends it to the
 * client.
 *
 */
IPC_HANDLER(get_bar_config) {
    /* If no ID was passed, we return a JSON array with all IDs */
    if (message_size == 0) {
        json_object *obj = json_object_new_array();

        Barconfig *current;
        TAILQ_FOREACH (current, &barconfigs, configs) {
            json_object_array_add(obj, json_object_new_string(current->id));
        }

        ipc_send_client_message(client, I3_IPC_REPLY_TYPE_BAR_CONFIG, obj);
        return;
    }

    /* To get a properly terminated buffer, we copy
     * message_size bytes out of the buffer */
    char *bar_id = NULL;
    sasprintf(&bar_id, "%.*s", message_size, message);
    LOG("IPC: looking for config for bar ID \"%s\"\n", bar_id);
    Barconfig *current, *config = NULL;
    TAILQ_FOREACH (current, &barconfigs, configs) {
        if (strcmp(current->id, bar_id) != 0) {
            continue;
        }

        config = current;
        break;
    }
    free(bar_id);

    json_object *obj;
    if (!config) {
        /* If we did not find a config for the given ID, the reply will contain
         * a null 'id' field. */
        obj = json_object_new_object();
        json_object_object_add(obj, "id", NULL);
    } else {
        obj = dump_bar_config(config);
    }

    ipc_send_client_message(client, I3_IPC_REPLY_TYPE_BAR_CONFIG, obj);
}

/*
 * Returns a list of configured binding modes
 *
 */
IPC_HANDLER(get_binding_modes) {
    json_object *obj = json_object_new_array();
    struct Mode *mode;
    SLIST_FOREACH (mode, &modes, modes) {
        json_object_array_add(obj, json_object_new_string(mode->name));
    }
    ipc_send_client_message(client, I3_IPC_REPLY_TYPE_BINDING_MODES, obj);
}

/*
 * Callback for the YAJL parser (will be called when a string is parsed).
 *
 */
static int add_subscription(void *extra, const unsigned char *s,
                            ylength len) {
    ipc_client *client = extra;

    DLOG("should add subscription to extra %p, sub %.*s\n", client, (int)len, s);
    int event = client->num_events;

    client->num_events++;
    client->events = srealloc(client->events, client->num_events * sizeof(char *));
    /* We copy the string because it is not null-terminated and strndup()
     * is missing on some BSD systems */
    client->events[event] = scalloc(len + 1, 1);
    memcpy(client->events[event], s, len);

    DLOG("client is now subscribed to:\n");
    for (int i = 0; i < client->num_events; i++) {
        DLOG("event %s\n", client->events[i]);
    }
    DLOG("(done)\n");

    return 1;
}

/*
 * Subscribes this connection to the event types which were given as a JSON
 * serialized array in the payload field of the message.
 *
 */
IPC_HANDLER(subscribe) {
    yajl_handle p;
    yajl_status stat;

    /* Setup the JSON parser */
    static yajl_callbacks callbacks = {
        .yajl_string = add_subscription,
    };

    p = yalloc(&callbacks, (void *)client);
    stat = yajl_parse(p, (const unsigned char *)message, message_size);
    if (stat != yajl_status_ok) {
        unsigned char *err;
        err = yajl_get_error(p, true, (const unsigned char *)message,
                             message_size);
        ELOG("YAJL parse error: %s\n", err);
        yajl_free_error(p, err);

        ipc_send_client_message(client, I3_IPC_REPLY_TYPE_SUBSCRIBE, dump_success(false));
        yajl_free(p);
        return;
    }
    yajl_free(p);
    ipc_send_client_message(client, I3_IPC_REPLY_TYPE_SUBSCRIBE, dump_success(true));

    if (client->first_tick_sent) {
        return;
    }

    bool is_tick = false;
    for (int i = 0; i < client->num_events; i++) {
        if (strcmp(client->events[i], "tick") == 0) {
            is_tick = true;
            break;
        }
    }
    if (!is_tick) {
        return;
    }

    client->first_tick_sent = true;
    json_object *obj = json_object_new_object();
    json_object_object_add(obj, "first", json_object_new_boolean(true));
    json_object_object_add(obj, "payload", json_object_new_string(""));
    ipc_send_client_message(client, I3_IPC_EVENT_TICK, obj);
}

/*
 * Returns the raw last loaded i3 configuration file contents.
 */
IPC_HANDLER(get_config) {
    json_object *obj = json_object_new_object();

    IncludedFile *file = TAILQ_FIRST(&included_files);
    json_object_object_add(obj, "config",
                           json_object_new_string(file->raw_contents));

    json_object *included_configs = json_object_new_array();
    json_object_object_add(obj, "included_configs", included_configs);
    TAILQ_FOREACH (file, &included_files, files) {
        json_object *config_obj = json_object_new_object();
        json_object_array_add(included_configs, config_obj);
        json_object_object_add(config_obj, "path",
                               json_object_new_string(file->path));
        json_object_object_add(config_obj, "raw_contents",
                               json_object_new_string(file->raw_contents));
        json_object_object_add(config_obj, "variable_replaced_contents",
                               json_object_new_string(file->variable_replaced_contents));
    }

    ipc_send_client_message(client, I3_IPC_REPLY_TYPE_CONFIG, obj);
}

/*
 * Sends the tick event from the message payload to subscribers. Establishes a
 * synchronization point in event-related tests.
 */
IPC_HANDLER(send_tick) {
    json_object *obj = json_object_new_object();
    json_object_object_add(obj, "first",
                           json_object_new_boolean(false));
    json_object_object_add(obj, "payload",
                           json_object_new_string_len((const char *)message, message_size));
    ipc_send_event("tick", I3_IPC_EVENT_TICK, obj);

    ipc_send_client_message(client, I3_IPC_REPLY_TYPE_TICK, dump_success(true));
    DLOG("Sent tick event\n");
}

struct sync_state {
    char *last_key;
    uint32_t rnd;
    xcb_window_t window;
};

static int _sync_json_key(void *extra, const unsigned char *val, size_t len) {
    struct sync_state *state = extra;
    FREE(state->last_key);
    state->last_key = scalloc(len + 1, 1);
    memcpy(state->last_key, val, len);
    return 1;
}

static int _sync_json_int(void *extra, long long val) {
    struct sync_state *state = extra;
    if (strcasecmp(state->last_key, "rnd") == 0) {
        state->rnd = val;
    } else if (strcasecmp(state->last_key, "window") == 0) {
        state->window = (xcb_window_t)val;
    }
    return 1;
}

IPC_HANDLER(sync) {
    yajl_handle p;
    yajl_status stat;

    /* Setup the JSON parser */
    static yajl_callbacks callbacks = {
        .yajl_map_key = _sync_json_key,
        .yajl_integer = _sync_json_int,
    };

    struct sync_state state;
    memset(&state, '\0', sizeof(struct sync_state));
    p = yalloc(&callbacks, (void *)&state);
    stat = yajl_parse(p, (const unsigned char *)message, message_size);
    FREE(state.last_key);
    if (stat != yajl_status_ok) {
        unsigned char *err;
        err = yajl_get_error(p, true, (const unsigned char *)message,
                             message_size);
        ELOG("YAJL parse error: %s\n", err);
        yajl_free_error(p, err);

        ipc_send_client_message(client, I3_IPC_REPLY_TYPE_SYNC, dump_success(false));
        yajl_free(p);
        return;
    }
    yajl_free(p);

    DLOG("received IPC sync request (rnd = %d, window = 0x%08x)\n", state.rnd, state.window);
    sync_respond(state.window, state.rnd);
    ipc_send_client_message(client, I3_IPC_REPLY_TYPE_SYNC, dump_success(true));
}

IPC_HANDLER(get_binding_state) {
    json_object *obj = json_object_new_object();
    json_object_object_add(obj, "name", json_object_new_string(current_binding_mode));
    ipc_send_client_message(client, I3_IPC_REPLY_TYPE_GET_BINDING_STATE, obj);
}

/* The index of each callback function corresponds to the numeric
 * value of the message type (see include/i3/ipc.h) */
handler_t handlers[13] = {
    handle_run_command,
    handle_get_workspaces,
    handle_subscribe,
    handle_get_outputs,
    handle_tree,
    handle_get_marks,
    handle_get_bar_config,
    handle_get_version,
    handle_get_binding_modes,
    handle_get_config,
    handle_send_tick,
    handle_sync,
    handle_get_binding_state,
};

/*
 * Handler for activity on a client connection, receives a message from a
 * client.
 *
 * For now, the maximum message size is 2048. I’m not sure for what the
 * IPC interface will be used in the future, thus I’m not implementing a
 * mechanism for arbitrarily long messages, as it seems like overkill
 * at the moment.
 *
 */
static void ipc_receive_message(EV_P_ struct ev_io *w, int revents) {
    uint32_t message_type;
    uint32_t message_length;
    uint8_t *message = NULL;
    ipc_client *client = (ipc_client *)w->data;
    assert(client->fd == w->fd);

    int ret = ipc_recv_message(w->fd, &message_type, &message_length, &message);
    /* EOF or other error */
    if (ret < 0) {
        /* Was this a spurious read? See ev(3) */
        if (ret == -1 && errno == EAGAIN) {
            FREE(message);
            return;
        }

        /* If not, there was some kind of error. We don’t bother and close the
         * connection. Delete the client from the list of clients. */
        free_ipc_client(client, -1);
        FREE(message);
        return;
    }

    if (message_type >= (sizeof(handlers) / sizeof(handler_t))) {
        DLOG("Unhandled message type: %d\n", message_type);
    } else {
        handler_t h = handlers[message_type];
        h(client, message, 0, message_length, message_type);
    }

    FREE(message);
}

static void ipc_client_timeout(EV_P_ ev_timer *w, int revents) {
    /* No need to be polite and check for writeability, the other callback would
     * have been called by now. */
    ipc_client *client = (ipc_client *)w->data;

    char *cmdline = NULL;
#if defined(__linux__) && defined(SO_PEERCRED)
    struct ucred peercred;
    socklen_t so_len = sizeof(peercred);
    if (getsockopt(client->fd, SOL_SOCKET, SO_PEERCRED, &peercred, &so_len) != 0) {
        goto end;
    }
    char *exepath;
    sasprintf(&exepath, "/proc/%d/cmdline", peercred.pid);

    int fd = open(exepath, O_RDONLY);
    free(exepath);
    if (fd == -1) {
        goto end;
    }
    char buf[512] = {'\0'}; /* cut off cmdline for the error message. */
    const ssize_t n = read(fd, buf, sizeof(buf));
    close(fd);
    if (n < 0) {
        goto end;
    }
    for (char *walk = buf; walk < buf + n - 1; walk++) {
        if (*walk == '\0') {
            *walk = ' ';
        }
    }
    cmdline = buf;

    if (cmdline) {
        ELOG("client %p with pid %d and cmdline '%s' on fd %d timed out, killing\n", client, peercred.pid, cmdline, client->fd);
    }

end:
#endif
    if (!cmdline) {
        ELOG("client %p on fd %d timed out, killing\n", client, client->fd);
    }

    free_ipc_client(client, -1);
}

static void ipc_socket_writeable_cb(EV_P_ ev_io *w, int revents) {
    DLOG("fd %d writeable\n", w->fd);
    ipc_client *client = (ipc_client *)w->data;

    /* If this callback is called then there should be a corresponding active
     * timer. */
    assert(client->timeout != NULL);
    ipc_push_pending(client);
}

/*
 * Handler for activity on the listening socket, meaning that a new client
 * has just connected and we should accept() him. Sets up the event handler
 * for activity on the new connection and inserts the file descriptor into
 * the list of clients.
 *
 */
void ipc_new_client(EV_P_ struct ev_io *w, int revents) {
    struct sockaddr_un peer;
    socklen_t len = sizeof(struct sockaddr_un);
    int fd;
    if ((fd = accept(w->fd, (struct sockaddr *)&peer, &len)) < 0) {
        if (errno != EINTR) {
            perror("accept()");
        }
        return;
    }

    /* Close this file descriptor on exec() */
    (void)fcntl(fd, F_SETFD, FD_CLOEXEC);

    ipc_new_client_on_fd(EV_A_ fd);
}

/*
 * ipc_new_client_on_fd() only sets up the event handler
 * for activity on the new connection and inserts the file descriptor into
 * the list of clients.
 *
 * This variant is useful for the inherited IPC connection when restarting.
 *
 */
ipc_client *ipc_new_client_on_fd(EV_P_ int fd) {
    set_nonblock(fd);

    ipc_client *client = scalloc(1, sizeof(ipc_client));
    client->fd = fd;

    client->read_callback = scalloc(1, sizeof(struct ev_io));
    client->read_callback->data = client;
    ev_io_init(client->read_callback, ipc_receive_message, fd, EV_READ);
    ev_io_start(EV_A_ client->read_callback);

    client->write_callback = scalloc(1, sizeof(struct ev_io));
    client->write_callback->data = client;
    ev_io_init(client->write_callback, ipc_socket_writeable_cb, fd, EV_WRITE);

    DLOG("IPC: new client connected on fd %d\n", fd);
    TAILQ_INSERT_TAIL(&all_clients, client, clients);
    return client;
}

/*
 * Generates a json workspace event. Returns a dynamically allocated yajl
 * generator. Free with yajl_gen_free().
 */
json_object *ipc_marshal_workspace_event(const char *change, Con *current, Con *old) {
    json_object *obj = json_object_new_object();

    json_object_object_add(obj, "change", json_object_new_string(change));

    if (current != NULL) {
        json_object_object_add(obj, "current", dump_node(current, false));
    }

    if (old != NULL) {
        json_object_object_add(obj, "old", dump_node(old, false));
    }

    return obj;
}

/*
 * For the workspace events we send, along with the usual "change" field, also
 * the workspace container in "current". For focus events, we send the
 * previously focused workspace in "old".
 */
void ipc_send_workspace_event(const char *change, Con *current, Con *old) {
    ipc_send_event("workspace", I3_IPC_EVENT_WORKSPACE,
                   ipc_marshal_workspace_event(change, current, old));
}

/*
 * For the window events we send, along the usual "change" field,
 * also the window container, in "container".
 */
void ipc_send_window_event(const char *property, Con *con) {
    DLOG("Issue IPC window %s event (con = %p, window = 0x%08x)\n",
         property, con, (con->window ? con->window->id : XCB_WINDOW_NONE));

    json_object *obj = json_object_new_object();
    json_object_object_add(obj, "change", json_object_new_string(property));
    json_object_object_add(obj, "container", dump_node(con, false));
    ipc_send_event("window", I3_IPC_EVENT_WINDOW, obj);
}

/*
 * For the barconfig update events, we send the serialized barconfig.
 */
void ipc_send_barconfig_update_event(Barconfig *barconfig) {
    DLOG("Issue barconfig_update event for id = %s\n", barconfig->id);
    ipc_send_event("barconfig_update", I3_IPC_EVENT_BARCONFIG_UPDATE,
                   dump_bar_config(barconfig));
}

/*
 * For the binding events, we send the serialized binding struct.
 */
void ipc_send_binding_event(const char *event_type, Binding *bind, const char *modename) {
    DLOG("Issue IPC binding %s event (sym = %s, code = %d)\n", event_type, bind->symbol, bind->keycode);

    json_object *obj = json_object_new_object();
    json_object_object_add(obj, "change", json_object_new_string(event_type));
    json_object_object_add(obj, "mode",
                           json_object_new_string(modename ? modename : "default"));
    json_object_object_add(obj, "binding", dump_binding(bind));
    ipc_send_event("binding", I3_IPC_EVENT_BINDING, obj);
}

/*
 * Sends a restart reply to the IPC client on the specified fd.
 */
void ipc_confirm_restart(ipc_client *client) {
    DLOG("ipc_confirm_restart(fd %d)\n", client->fd);
    ipc_send_client_message(client, I3_IPC_REPLY_TYPE_COMMAND, dump_success(true));
    ipc_push_pending(client);
}
