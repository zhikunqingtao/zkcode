//! Remove annotation-only schema fields without weakening validation constraints.
//!
//! Walk schema positions explicitly: values under `enum`, `const`, `default`,
//! or extension keywords may be user data and must never be interpreted as schemas.

use serde_json::Value;

/// Produce a compact model-facing schema. The original server schema remains
/// on the connection for protocol discovery. Enum values, references, bounds,
/// required fields and composition branches are preserved without truncation.
#[must_use]
pub fn compact_schema(mut schema: Value) -> Value {
    compact_node(&mut schema);
    schema
}

fn compact_node(schema: &mut Value) {
    let Some(object) = schema.as_object_mut() else {
        return;
    };
    for annotation in ["$comment", "examples", "title"] {
        object.remove(annotation);
    }
    for map_keyword in [
        "properties",
        "patternProperties",
        "$defs",
        "definitions",
        "dependentSchemas",
    ] {
        if let Some(Value::Object(children)) = object.get_mut(map_keyword) {
            for child in children.values_mut() {
                compact_node(child);
            }
        }
    }
    for schema_keyword in [
        "additionalProperties",
        "additionalItems",
        "contains",
        "propertyNames",
        "not",
        "if",
        "then",
        "else",
        "unevaluatedProperties",
        "unevaluatedItems",
        "contentSchema",
    ] {
        if let Some(child) = object.get_mut(schema_keyword) {
            compact_node(child);
        }
    }
    for array_keyword in ["allOf", "anyOf", "oneOf", "prefixItems"] {
        if let Some(Value::Array(children)) = object.get_mut(array_keyword) {
            for child in children {
                compact_node(child);
            }
        }
    }
    if let Some(items) = object.get_mut("items") {
        if let Value::Array(children) = items {
            for child in children {
                compact_node(child);
            }
        } else {
            compact_node(items);
        }
    }
    // Draft-07 dependencies can contain either schemas or string arrays.
    if let Some(Value::Object(dependencies)) = object.get_mut("dependencies") {
        for dependency in dependencies.values_mut() {
            compact_node(dependency);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn strips_annotations_only_at_schema_positions_and_preserves_all_constraints() {
        let schema = json!({
            "$id":"https://example.com/schema", "$schema":"https://json-schema.org/draft/2020-12/schema",
            "title":"Root", "$comment":"internal documentation", "examples":[{}],
            "description":"Exactly one of the following legal values is required.",
            "type":"object", "required":["mode"], "additionalProperties":false,
            "properties": {"mode":{"title":"Mode", "enum":["a","b","c","d","e","f","g"]},
                "data":{"const":{"title":"this is data", "examples":[1]}, "default":{"title":"data"}},
                "number":{"minimum":3,"maximum":9,"multipleOf":3}},
            "$defs":{"item":{"title":"Item", "type":"string","minLength":1,"pattern":"^x"}},
            "allOf":[{"title":"Rule", "if":{"required":["number"]}, "then":{"required":["mode"]}}],
            "x-extension":{"title":"opaque extension value"}
        });
        let compact = compact_schema(schema.clone());
        let mut expected = schema.clone();
        for key in ["title", "$comment", "examples"] {
            expected.as_object_mut().unwrap().remove(key);
        }
        expected["properties"]["mode"]
            .as_object_mut()
            .unwrap()
            .remove("title");
        expected["$defs"]["item"]
            .as_object_mut()
            .unwrap()
            .remove("title");
        expected["allOf"][0]
            .as_object_mut()
            .unwrap()
            .remove("title");
        assert_eq!(compact, expected);
        assert_eq!(compact_schema(compact.clone()), compact);
        assert!(compact.to_string().len() < schema.to_string().len());
    }

    #[test]
    fn preserves_boolean_schemas_reference_siblings_and_property_names() {
        let schema = json!({"properties":{"title":{"title":"annotation","$ref":"#/$defs/a","minLength":2}},"$defs":{"a":false},"additionalProperties":true});
        let compact = compact_schema(schema);
        assert_eq!(
            compact["properties"]["title"],
            json!({"$ref":"#/$defs/a","minLength":2})
        );
        assert_eq!(compact["$defs"]["a"], false);
        assert_eq!(compact["additionalProperties"], true);
    }
}
