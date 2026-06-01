use uuid::Uuid;

use crate::error::Result;

#[derive(Clone, Debug, Default)]
pub struct IdGenerator;

impl IdGenerator {
    pub fn new() -> Self {
        Self
    }

    pub fn new_id(&self) -> Result<String> {
        Ok(Uuid::now_v7().to_string())
    }
}

pub fn is_safe_identifier(identifier: &str) -> bool {
    if identifier.is_empty() || identifier.len() > 128 {
        return false;
    }
    identifier.chars().all(|character| {
        character.is_ascii_alphanumeric() || matches!(character, '-' | '_' | '.' | ':')
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generator_creates_uuid_v7() {
        let identifier = IdGenerator::new().new_id().expect("id");

        assert_eq!(identifier.len(), 36);
        assert_eq!(identifier.as_bytes()[14], b'7');
        assert!(matches!(
            identifier.as_bytes()[19],
            b'8' | b'9' | b'a' | b'b'
        ));
        assert!(is_safe_identifier(&identifier));
    }

    #[test]
    fn safe_identifier_rejects_slash() {
        assert!(!is_safe_identifier("abc/def"));
    }
}
