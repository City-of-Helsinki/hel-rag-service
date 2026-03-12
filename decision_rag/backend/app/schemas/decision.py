"""
Pydantic models for decision API responses.
"""

from typing import List, Optional

from pydantic import BaseModel, Field


class DecisionId(BaseModel):
    """Individual decision ID item from the decision list endpoint."""

    NativeId: str
    Title: Optional[str] = None
    CaseIDLabel: Optional[str] = None
    Section: Optional[str] = None
    CaseID: Optional[str] = None


class DecisionIdResponse(BaseModel):
    """Response model for decision ID endpoint."""

    decisions: List[DecisionId] = Field(default_factory=list)
    page: Optional[int] = None
    count: Optional[int] = None
    nextCount: Optional[int] = None
    prevCount: Optional[int] = None
    size: Optional[int] = None


class Attachment(BaseModel):
    """Attachment information."""

    Title: Optional[str] = None
    Type: Optional[str] = None
    FileURI: Optional[str] = None
    PublicityClass: Optional[str] = None
    AttachmentNumber: Optional[int] = None
    NativeId: Optional[str] = None
    SecurityReasons: Optional[List[str]] = Field(default_factory=list)
    PersonalData: Optional[str] = None
    Language: Optional[str] = None


class SectorInfo(BaseModel):
    Sector: Optional[str] = None

class OrganizationInfo(BaseModel):
    """Organization information."""

    Name: Optional[str] = None
    ID: Optional[str] = None
    TypeId: Optional[str] = None
    Type: Optional[str] = None
    Sector: Optional[SectorInfo] = None


class DecisionDocument(BaseModel):
    """Complete decision document with all fields."""

    NativeId: str
    Title: Optional[str] = None
    CaseIDLabel: Optional[str] = None
    Section: Optional[str] = None
    Content: Optional[str] = None  # HTML content
    ClassificationCode: Optional[str] = None
    ClassificationTitle: Optional[str] = None
    Organization: Optional["OrganizationInfo"] = None
    Attachments: Optional[List[Attachment]] = Field(default_factory=list)
    DateDecision: Optional[str] = None
    CaseID: Optional[str] = None


class DecisionDocumentResponse(BaseModel):
    """Response model for decision document endpoint."""

    decisions: List[DecisionDocument] = Field(default_factory=list)


class DecisionMetadata(BaseModel):
    """Extracted metadata (all fields except Content) for filtering and retrieval."""

    NativeId: str
    Title: Optional[str] = None
    CaseIDLabel: Optional[str] = None
    Section: Optional[str] = None
    Motion: Optional[str] = None
    ClassificationCode: Optional[str] = None
    ClassificationTitle: Optional[str] = None
    Organization: Optional[OrganizationInfo] = None
    Attachments: Optional[List[Attachment]] = Field(default_factory=list)
    DateDecision: Optional[str] = None
    CaseID: Optional[str] = None

    @classmethod
    def from_decision_document(cls, doc: DecisionDocument) -> "DecisionMetadata":
        """Extract metadata from a DecisionDocument."""
        return cls(
            NativeId=doc.NativeId,
            Title=doc.Title,
            CaseIDLabel=doc.CaseIDLabel,
            ClassificationCode=doc.ClassificationCode,
            ClassificationTitle=doc.ClassificationTitle,
            Organization=doc.Organization,
            Attachments=doc.Attachments,
            DateDecision=doc.DateDecision,
            CaseID=doc.CaseID,
        )


class AttachmentChunkMetadata(BaseModel):
    """Metadata for attachment chunks to distinguish them from decision chunks."""

    attachment_native_id: Optional[str] = None
    attachment_title: Optional[str] = None
    attachment_number: Optional[int] = None
    attachment_type: Optional[str] = None
    attachment_url: Optional[str] = None
    decision_native_id: str
    is_attachment: bool = True
