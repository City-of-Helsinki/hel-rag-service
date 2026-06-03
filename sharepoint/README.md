# SharePoint Integration PoC

A proof-of-concept tool that crawls multiple SharePoint sites via the Microsoft Graph API and converts all pages and files (PDF, DOCX, PPTX, XLSX) into Markdown documents saved to local disk.

> **Intended use:** Testing and exploration. The TODO in `main.py` notes the next step is pushing the Markdown files into a vector store as part of a data pipeline.

---

## Prerequisites

- Python 3.11+
- An Azure AD **App Registration** with the following Microsoft Graph **application** permissions:
  - `Sites.Selected`

---

## Setup

**1. Clone and install dependencies**

```bash
pip install -r requirements.txt
```

**2. Create a `.env` file** in the project root with Azure AD credentials:

```env
SHAREPOINT_TENANT_ID=your-tenant-id
SHAREPOINT_CLIENT_ID=your-app-client-id
SHAREPOINT_CLIENT_SECRET=your-app-client-secret
```

**3. Configure sites in `sites_config.yaml`:**

```yaml
output_dir: output

file_extensions:
  - .pdf
  - .docx
  - .pptx
  - .xlsx

sites:
  service1:
    name: "Your sites"
    urls:
      - https://tenant.sharepoint.com/sites/your-site

  service2:
    name: "Your sites 2"
    urls:
      - https://tenant.sharepoint.com/sites/your-site-2
      - https://tenant.sharepoint.com/sites/your-sites-2-1
```

---

## Usage

```bash
# Crawl all configured sites
python main.py

# Crawl specific site group(s)
python main.py --sites service1
python main.py --sites service2
python main.py --sites service1 service2

# List configured sites
python main.py --list

# Use a custom config file
python main.py --config /path/to/my_config.yaml

# Define markitdown as converter (default is docling)
python main.py --converter markitdown
```

The script will:
1. Authenticate against Azure AD using client credentials (app-only).
2. Crawl all SharePoint pages and supported files from the selected sites.
3. Convert content to Markdown using [docling](https://github.com/DS4SD/docling).
4. Save results under `output/<site-name>/`:
   - `output/<site-name>/pages/` — site pages as `.md` files
   - `output/<site-name>/files/` — converted documents as `.md` + `.json` metadata files

---

## Output

Each site gets its own subdirectory nested under the group name:

```
output/
├── service1/
│   └── your-site/
│       ├── pages/
│       │   └── instructions.md
│       └── files/
│           ├── archive-2019.md
│           └── archive-2019.json
└── service2/
    ├── your-site-2/
    │   ├── pages/
    │   └── files/
    └── your-site-2-1/
        ├── pages/
        └── files/
```

---

## Connection test

To verify credentials and connectivity without running a full crawl:

```bash
python connection_test.py
```

