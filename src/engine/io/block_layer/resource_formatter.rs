use uuid::Uuid;

/// Number of characters per folder
const PREFIX_LEN: usize = 2;

pub struct ResourceFormatter {}

impl ResourceFormatter {
    pub fn format_uuid(input: &Uuid) -> String {
        let mut buf = [b'0'; 32];
        input.to_simple().encode_lower(&mut buf);
        String::from_utf8_lossy(&buf).into_owned()
    }

    pub fn get_uuid_prefix(input: &Uuid) -> String {
        let mut buf = [b'0'; 32];
        input.to_simple().encode_lower(&mut buf);
        String::from_utf8_lossy(&buf[..PREFIX_LEN]).into_owned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hex_literal::hex;

    #[test]
    fn test_uuid_formating() -> Result<(), Box<dyn std::error::Error>> {
        let hex = "ee89957f3e9f482c836dda6c349ac632";
        let test = Uuid::from_bytes(hex!("ee89957f3e9f482c836dda6c349ac632"));
        assert_eq!(hex, ResourceFormatter::format_uuid(&test));

        assert_eq!("ee", ResourceFormatter::get_uuid_prefix(&test));

        Ok(())
    }
}
