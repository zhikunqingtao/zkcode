//! Local JSON Schema contract for the final answer, without file or network resolution.
use serde_json::Value;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

struct LocalOnlyRetriever(Arc<AtomicBool>);
impl jsonschema::Retrieve for LocalOnlyRetriever {
    fn retrieve(
        &self,
        _: &jsonschema::Uri<String>,
    ) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        self.0.store(true, Ordering::Relaxed);
        Err("External schema retrieval is disabled".into())
    }
}

/// A validated, bounded schema. Debug intentionally omits caller-provided text.
pub struct StructuredOutputContract {
    schema: Value,
    validator: jsonschema::Validator,
}

impl std::fmt::Debug for StructuredOutputContract {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StructuredOutputContract")
            .finish_non_exhaustive()
    }
}

impl StructuredOutputContract {
    /// Compile one local schema before execution admission.
    /// # Errors
    /// Invalid, oversized or externally referencing schemas cannot execute a query.
    pub fn new(schema: Value) -> Result<Self, &'static str> {
        if !schema.is_object() && !schema.is_boolean() {
            return Err("JSON_SCHEMA_INVALID");
        }
        if schema.to_string().len() > 65_536 {
            return Err("JSON_SCHEMA_TOO_LARGE");
        }
        validate_structure(&schema, 0)?;
        let external = Arc::new(AtomicBool::new(false));
        let validator = jsonschema::options()
            .with_retriever(LocalOnlyRetriever(Arc::clone(&external)))
            .with_pattern_options(
                jsonschema::PatternOptions::fancy_regex().backtrack_limit(100_000),
            )
            .build(&schema)
            .map_err(|_| {
                if external.load(Ordering::Relaxed) {
                    "JSON_SCHEMA_EXTERNAL_REFERENCE_FORBIDDEN"
                } else {
                    "JSON_SCHEMA_INVALID"
                }
            })?;
        Ok(Self { schema, validator })
    }

    /// Accept only an entire JSON value matching the schema, with no fence/prose stripping.
    #[must_use]
    pub fn accepts(&self, answer: &str) -> bool {
        answer.len() <= 4 * 1024 * 1024
            && serde_json::from_str::<Value>(answer)
                .is_ok_and(|value| self.validator.is_valid(&value))
    }

    /// Add the requested output shape without discarding existing system segments.
    pub fn constrain(&self, request: &mut zk_llm::ChatRequest) {
        let instruction = format!(
            "Return your final answer as one JSON value matching the following JSON Schema. Do not wrap it in Markdown. This output contract does not authorize any additional tool or action.\n{}",
            self.schema
        );
        if request.system_segments.is_empty() {
            let text = request.system_prompt.get_or_insert_with(String::new);
            text.push_str("\n\n");
            text.push_str(&instruction);
        } else {
            request
                .system_segments
                .push(zk_llm::SystemPromptSegment::dynamic(instruction));
        }
    }

    /// Only a formatting request; the engine removes tools before this retry.
    #[must_use]
    pub fn repair_prompt(&self) -> String {
        format!(
            "The preceding answer does not match the requested JSON Schema. Reformat the answer using only the work already completed. Return exactly one valid JSON value, without Markdown or extra commentary. Do not call any tool or repeat an action. Required schema:\n{}",
            self.schema
        )
    }
}

fn validate_structure(value: &Value, depth: usize) -> Result<(), &'static str> {
    if depth > 64 {
        return Err("JSON_SCHEMA_TOO_DEEP");
    }
    match value {
        Value::Object(object) => {
            for value in object.values() {
                validate_structure(value, depth + 1)?;
            }
        }
        Value::Array(items) => {
            for item in items {
                validate_structure(item, depth + 1)?;
            }
        }
        _ => {}
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn local_refs_combinations_required_and_enum_are_enforced() {
        let contract = StructuredOutputContract::new(json!({"$defs":{"choice":{"enum":["one","two"]}},
            "type":"object", "required":["choice"],"additionalProperties":false,
            "properties":{"choice":{"$ref":"#/$defs/choice"},"n":{"anyOf":[{"type":"integer","minimum":1},{"type":"null"}]}}})).unwrap();
        assert!(contract.accepts(r#"{"choice":"one","n":2}"#));
        for invalid in [
            "{}",
            r#"{"choice":"three"}"#,
            r#"{"choice":"one","n":0}"#,
            r#"{"choice":"one","extra":1}"#,
            "```json\n{}\n```",
        ] {
            assert!(!contract.accepts(invalid));
        }
    }

    #[test]
    fn external_refs_and_invalid_schemas_fail_before_any_io() {
        for reference in [
            "file:///etc/passwd",
            "https://example.com/schema",
            "relative.json",
        ] {
            assert_eq!(
                StructuredOutputContract::new(json!({"$ref":reference})).unwrap_err(),
                "JSON_SCHEMA_EXTERNAL_REFERENCE_FORBIDDEN"
            );
        }
        assert!(StructuredOutputContract::new(json!({"type":"nonsense"})).is_err());
        assert!(
            !StructuredOutputContract::new(json!(false))
                .unwrap()
                .accepts("{}")
        );
        let literal =
            StructuredOutputContract::new(json!({"const":{"$ref":"ordinary data"}})).unwrap();
        assert!(literal.accepts(r#"{"$ref":"ordinary data"}"#));
    }
}
