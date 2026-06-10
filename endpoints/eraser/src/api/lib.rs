pub mod render_elements;
pub mod render_prompt;

// ── File CRUD ────────────────────────────────────────────────────────────────
pub mod archive_file;
pub mod create_file;
pub mod get_file;
pub mod list_files;
pub mod update_file;

// ── Diagram CRUD ─────────────────────────────────────────────────────────────
pub mod create_diagram;
pub mod delete_diagram;
pub mod get_diagram;
pub mod list_diagrams;
pub mod update_diagram;

use serde::{Deserialize, Serialize};
use std::fmt::Display;

#[derive(Debug, Serialize, Deserialize, Clone, utoipa::ToSchema)]
pub enum EraserApi {
    // Render
    RenderPrompt,
    RenderElements,

    // File CRUD
    CreateFile,
    ListFiles,
    GetFile,
    UpdateFile,
    ArchiveFile,

    // Diagram CRUD
    CreateDiagram,
    ListDiagrams,
    GetDiagram,
    UpdateDiagram,
    DeleteDiagram,
}

impl Display for EraserApi {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::RenderPrompt => write!(f, "render_prompt"),
            Self::RenderElements => write!(f, "render_elements"),
            Self::CreateFile => write!(f, "create_file"),
            Self::ListFiles => write!(f, "list_files"),
            Self::GetFile => write!(f, "get_file"),
            Self::UpdateFile => write!(f, "update_file"),
            Self::ArchiveFile => write!(f, "archive_file"),
            Self::CreateDiagram => write!(f, "create_diagram"),
            Self::ListDiagrams => write!(f, "list_diagrams"),
            Self::GetDiagram => write!(f, "get_diagram"),
            Self::UpdateDiagram => write!(f, "update_diagram"),
            Self::DeleteDiagram => write!(f, "delete_diagram"),
        }
    }
}
