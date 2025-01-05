/*
 * vim:ts=4:sw=4:expandtab
 *
 * i3 - an improved tiling window manager
 * © 2025 Michael Stapelberg and contributors (see also: LICENSE)
 *
 * json_utils.c
 *
 */
#include "libi3.h"

#include <json-c/json_object.h>
#include <json-c/json_tokener.h>

/*
 * Parse a JSON string.
 *
 */
json_object *json_parse(const uint8_t *data, size_t len) {
    json_tokener *tok = json_tokener_new();
    json_tokener_set_flags(tok, JSON_TOKENER_STRICT);
    json_object *obj = json_tokener_parse_ex(tok, (char *)data, len);
    json_tokener_free(tok);
    return obj;
}
