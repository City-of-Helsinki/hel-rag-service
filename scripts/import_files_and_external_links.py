import os
import sys
import json
import requests
import unicodedata
import ast


class OpenWebUIImporter:
    def __init__(self, base_url: str, api_key: str):
        self.base_url = base_url.rstrip('/')
        self.headers = {
            "Authorization": f"Bearer {api_key}"
        }

    def normalize_string(self, text: str) -> str:
        """Standardizes strings to lowercase, strips whitespace, and enforces NFC Unicode."""
        if not text:
            return ""
        return unicodedata.normalize('NFC', text).strip().lower()

    def _extract_metadata(self, f_obj: dict) -> dict:
        """Safely extracts and parses metadata from the API file object."""
        meta = f_obj.get("meta", {})
        if not meta:
            return {}

        data = meta.get("data", {})
        if isinstance(data, str):
            try:
                return json.loads(data)
            except json.JSONDecodeError:
                try:
                    return ast.literal_eval(data)
                except (ValueError, SyntaxError):
                    return {}

        return data if isinstance(data, dict) else {}

    # --- KNOWLEDGE BASE METHODS ---

    def get_knowledge_base_by_name(self, name: str) -> str:
        url = f"{self.base_url}/api/v1/knowledge/"
        print(f"Checking for existing Knowledge Base: '{name}'...")
        response = requests.get(url, headers=self.headers)
        response.raise_for_status()

        data = response.json()
        for item in data.get("items", []):
            if item.get("name") == name:
                return item.get("id")
        return None

    def create_knowledge_base(self, name: str, description: str) -> str:
        url = f"{self.base_url}/api/v1/knowledge/create"
        payload = {"name": name, "description": description, "access_grants": []}

        response = requests.post(url, headers=self.headers, json=payload)
        response.raise_for_status()

        kb_id = response.json().get("id")
        print(f"✅ Created new Knowledge Base with ID: {kb_id}")
        return kb_id

    # --- FILE METHODS ---

    def get_all_system_files(self) -> dict:
        """Fetches all uploaded files, mapping normalized_filename -> LIST of file objects."""
        print("Fetching existing system files...")
        system_files = {}
        page = 1
        seen_ids = set()

        while True:
            url = f"{self.base_url}/api/v1/files/"
            params = {"content": "false", "page": page}

            response = requests.get(url, headers=self.headers, params=params)
            response.raise_for_status()

            files_list = response.json()

            if not files_list or not isinstance(files_list, list):
                break

            new_items_found = False
            for f in files_list:
                file_id = f.get("id")
                filename = f.get("filename")

                if file_id and filename and file_id not in seen_ids:
                    seen_ids.add(file_id)
                    new_items_found = True

                    norm_name = self.normalize_string(filename)
                    if norm_name not in system_files:
                        system_files[norm_name] = []
                    system_files[norm_name].append(f)

            if not new_items_found:
                break

            page += 1

        print(f"✅ Loaded {len(seen_ids)} total system files.")
        return system_files

    def get_kb_files(self, kb_id: str) -> set:
        print(f"Fetching existing files for Knowledge Base {kb_id}...")
        kb_file_ids = set()
        page = 1

        while True:
            url = f"{self.base_url}/api/v1/knowledge/{kb_id}/files"
            params = {"page": page}

            response = requests.get(url, headers=self.headers, params=params)
            response.raise_for_status()

            data = response.json()
            items = data.get("items", [])

            if not items:
                break

            for item in items:
                file_id = item.get("id")
                if file_id:
                    kb_file_ids.add(file_id)

            page += 1

        print(f"✅ Loaded {len(kb_file_ids)} total file IDs from the Knowledge Base.")
        return kb_file_ids

    def upload_file(self, md_filepath: str, file_metadata: dict) -> str:
        url = f"{self.base_url}/api/v1/files/"
        headers = {
            'Authorization': self.headers['Authorization'],
            'Accept': 'application/json'
        }

        filename = os.path.basename(md_filepath)

        file_size = os.path.getsize(md_filepath)
        if file_size == 0:
            raise ValueError(f"File {filename} is 0 bytes on disk. Skipping upload.")

        print(f"  ⬆️ Uploading ({file_size} bytes)...")

        with open(md_filepath, 'rb') as f:
            files = {'file': (filename, f), 'metadata': (None, json.dumps(file_metadata), 'application/json')}
            response = requests.post(url, headers=headers, files=files,
                                     params={"process": "true", "process_in_background": "false"}, timeout=120)
            try:
                response.raise_for_status()
            except requests.exceptions.RequestException as e:
                print(f"  ❌ API rejected upload. Server says: {response.text}")
                raise e

        return response.json().get("id")

    def add_file_to_kb(self, kb_id: str, file_id: str):
        url = f"{self.base_url}/api/v1/knowledge/{kb_id}/file/add"
        payload = {"file_id": file_id, "source_id": file_id}
        response = requests.post(url, headers=self.headers, json=payload, timeout=30)
        response.raise_for_status()

    def delete_file(self, file_id: str):
        url = f"{self.base_url}/api/v1/files/{file_id}"
        response = requests.delete(url, headers=self.headers)
        response.raise_for_status()

    # --- DEDUPLICATION ---

    def deduplicate_kb_files(self, system_files: dict, kb_file_ids: set):
        """Finds and removes duplicate files with the same name inside the Knowledge Base."""
        print("\nChecking for duplicate files in the Knowledge Base...")
        deleted_count = 0

        for norm_name, f_objs in system_files.items():
            # Find all file objects for this name that are ACTUALLY linked to the KB
            kb_linked = [f for f in f_objs if f.get("id") in kb_file_ids]

            if len(kb_linked) > 1:
                print(f"  ⚠️ Found {len(kb_linked)} duplicates for '{norm_name}' in KB. Cleaning up...")

                good_files = []
                bad_files = []

                for f in kb_linked:
                    meta = self._extract_metadata(f)
                    has_all = bool(meta.get("webUrl")) and bool(meta.get("name")) and bool(
                        meta.get("lastModifiedDateTime"))
                    if has_all:
                        good_files.append(f)
                    else:
                        bad_files.append(f)

                files_to_delete = []

                if good_files:
                    # We have at least one good file. Keep the first good one, delete the rest.
                    files_to_delete.extend(good_files[1:])
                    files_to_delete.extend(bad_files)
                else:
                    # They are all bad. Keep one so the main loop can naturally upgrade it, delete the rest.
                    files_to_delete.extend(bad_files[1:])

                for f_del in files_to_delete:
                    del_id = f_del.get("id")
                    print(f"    🗑️ Deleting redundant file ID: {del_id}")
                    try:
                        self.delete_file(del_id)
                        deleted_count += 1
                        kb_file_ids.remove(del_id)
                        # Remove it from our local dictionary so we don't trip over it later
                        system_files[norm_name] = [f for f in system_files[norm_name] if f.get("id") != del_id]
                    except Exception as e:
                        print(f"    ❌ Failed to delete duplicate {del_id}: {e}")

        if deleted_count > 0:
            print(f"✅ Deduplication complete. Removed {deleted_count} redundant files.")
        else:
            print("✅ No duplicates found.")

    # --- ORCHESTRATION ---

    def process_directory(self, folder_path: str, kb_id: str):
        system_files = self.get_all_system_files()
        kb_file_ids = self.get_kb_files(kb_id)

        # 1. Run Deduplication first to clean up the KB state
        self.deduplicate_kb_files(system_files, kb_file_ids)

        # 2. Calculate the fully synced and fully compliant files
        fully_synced_filenames = set()

        for filename, f_objs in system_files.items():
            for f_obj in f_objs:
                if f_obj.get("id") in kb_file_ids:
                    meta_data = self._extract_metadata(f_obj)

                    has_webUrl = bool(meta_data.get("webUrl"))
                    has_name = bool(meta_data.get("name"))
                    has_lastMod = bool(meta_data.get("lastModifiedDateTime"))

                    if has_webUrl and has_name and has_lastMod:
                        fully_synced_filenames.add(filename)
                        break

        print(f"\n✅ Pre-check complete: {len(fully_synced_filenames)} unique files are completely up-to-date.")
        print(f"Scanning directory: {folder_path}...")

        skipped_count = 0
        processed_count = 0
        error_count = 0

        for root, _, files in os.walk(folder_path):
            for file in files:
                if file.endswith(".md"):
                    md_path = os.path.join(root, file)
                    base_name = file[:-3]
                    json_path = os.path.join(root, f"{base_name}.json")
                    filename = os.path.basename(md_path)

                    if not os.path.exists(json_path):
                        continue

                    try:
                        normalized_filename = self.normalize_string(filename)

                        if normalized_filename in fully_synced_filenames:
                            skipped_count += 1
                            continue

                        processed_count += 1
                        print(f"\n📄 Processing: {filename}")

                        # --- PRECISE CLEANUP PHASE ---
                        existing_f_objs = system_files.get(normalized_filename, [])
                        objs_to_keep = []

                        if existing_f_objs:
                            for f_obj in existing_f_objs:
                                old_id = f_obj.get("id")
                                if old_id in kb_file_ids:
                                    print(f"  ⚠️ Target KB file lacks metadata. Deleting old copy (ID: {old_id})...")
                                    try:
                                        self.delete_file(old_id)
                                        kb_file_ids.remove(old_id)
                                    except Exception as e:
                                        print(f"  ❌ Failed to delete old file {old_id}: {e}")
                                        objs_to_keep.append(f_obj)
                                else:
                                    objs_to_keep.append(f_obj)

                            system_files[normalized_filename] = objs_to_keep

                        # --- UPLOAD PHASE ---
                        with open(json_path, 'r', encoding='utf-8') as jf:
                            json_data = json.load(jf)

                            file_metadata = {
                                "webUrl": json_data.get("webUrl", ""),
                                "name": json_data.get("name", ""),
                                "lastModifiedDateTime": json_data.get("lastModifiedDateTime", "")
                            }

                        file_id = self.upload_file(md_path, file_metadata)

                        if normalized_filename not in system_files:
                            system_files[normalized_filename] = []
                        system_files[normalized_filename].append({"id": file_id})
                        print(f"  ✅ Uploaded successfully. ID: {file_id}")

                        # --- LINKING PHASE ---
                        print(f"  ➕ Linking to Knowledge Base...")
                        try:
                            self.add_file_to_kb(kb_id, file_id)
                            kb_file_ids.add(file_id)
                            fully_synced_filenames.add(normalized_filename)
                            print(f"  ✅ Linked successfully.")
                        except requests.exceptions.RequestException as e:
                            error_msg = e.response.text if e.response is not None else str(e)
                            print(f"  ❌ Linking failed. Server says: {error_msg}")
                            error_count += 1

                            try:
                                print(f"  🗑️ Cleaning up orphaned file {file_id}...")
                                self.delete_file(file_id)
                            except Exception as cleanup_e:
                                print(f"  ⚠️ Failed to delete corrupted file {file_id}: {cleanup_e}")

                    except Exception as e:
                        print(f"❌ Failed to process {filename}: {e}")
                        error_count += 1

        print("\n" + "=" * 40)
        print("🎉 Import/Sync Process Complete!")
        print(f"📊 Summary:")
        print(f"   - {skipped_count} files already perfectly synced (silently skipped)")
        print(f"   - {processed_count} files required action (uploaded or upgraded)")
        if error_count > 0:
            print(f"   - ⚠️ Encountered {error_count} errors during processing")
        print("=" * 40 + "\n")


# ==========================================
# Configuration & Execution
# ==========================================
if __name__ == "__main__":
    import argparse
    from datetime import datetime
    from pathlib import Path
    from dotenv import load_dotenv

    # Load .env from project root and scripts dir
    load_dotenv(Path(__file__).resolve().parent.parent / ".env", override=False)
    load_dotenv(override=False)

    parser = argparse.ArgumentParser(
        description="Import Markdown files with metadata into an Open WebUI Knowledge Base.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  # Auto-generated KB name: service1-instructions-29-04-2026\n"
            "  python import_files_and_external_links.py ../sharepoint/output/service1 --customer service1 --purpose instructions\n\n"
            "  # Explicit KB name\n"
            "  python import_files_and_external_links.py ../sharepoint/output/service1 --kb-name My-Custom-KB\n"
        ),
    )
    parser.add_argument("folder", help="Path to the folder containing .md and .json files to import.")
    parser.add_argument("--kb-name", default=None, help="Full name of the Knowledge Base. Overrides --customer/--purpose.")
    parser.add_argument("--customer", default=None, help="Customer name for auto-generated KB name (e.g. service1).")
    parser.add_argument("--purpose", default=None, help="Purpose tag for auto-generated KB name (e.g. instructions).")
    parser.add_argument("--kb-description", default=None, help="Description for the Knowledge Base (used only when creating a new one).")
    parser.add_argument("--base-url", default=None, help="Open WebUI base URL. Defaults to env OPEN_WEB_UI_BASE_URL.")
    parser.add_argument("--api-key", default=None, help="Open WebUI API key. Defaults to env OPEN_WEB_UI_API_KEY.")
    parser.add_argument("-y", "--yes", action="store_true", help="Skip confirmation prompt.")

    args = parser.parse_args()

    base_url = args.base_url or os.getenv("OPEN_WEB_UI_BASE_URL")
    api_key = args.api_key or os.getenv("OPEN_WEB_UI_API_KEY")

    if not base_url or not api_key:
        print("❌ Error: Missing Open WebUI credentials.")
        print("Provide --base-url / --api-key or set OPEN_WEB_UI_BASE_URL and OPEN_WEB_UI_API_KEY env vars.")
        sys.exit(1)

    # Resolve KB name
    if args.kb_name:
        kb_name = args.kb_name
    elif args.customer and args.purpose:
        date_str = datetime.now().strftime("%d-%m-%Y")
        kb_name = f"{args.customer.upper()}-{args.purpose}-{date_str}"
    else:
        print("❌ Error: Provide either --kb-name or both --customer and --purpose.")
        sys.exit(1)

    kb_desc = args.kb_description or f"Knowledge Base: {kb_name}"

    importer = OpenWebUIImporter(base_url=base_url, api_key=api_key)

    if not args.yes:
        print(f"\n⚠️  You are about to connect to: {base_url}")
        print(f"   Knowledge Base: {kb_name}")
        print(f"   Source folder:  {args.folder}\n")
        confirm = input("Are you sure you want to proceed? [y/N]: ").strip().lower()
        if confirm != "y":
            print("Aborted.")
            sys.exit(0)

    try:
        knowledge_base_id = importer.get_knowledge_base_by_name(kb_name)

        if knowledge_base_id:
            print(f"🔄 Found existing Knowledge Base '{kb_name}' with ID: {knowledge_base_id}")
        else:
            knowledge_base_id = importer.create_knowledge_base(name=kb_name, description=kb_desc)

        importer.process_directory(folder_path=args.folder, kb_id=knowledge_base_id)

    except requests.exceptions.RequestException as e:
        print(f"\n❌ API Error: {e}")
        if e.response is not None:
            print(f"Response Details: {e.response.text}")
        sys.exit(1)