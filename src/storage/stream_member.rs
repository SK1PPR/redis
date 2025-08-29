#[derive(Debug, Clone)]
pub struct StreamMember {
    pub id: String,
    pub fields: Vec<(String, String)>,
}