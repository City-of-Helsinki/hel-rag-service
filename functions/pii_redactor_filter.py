
from typing import Optional
from pydantic import BaseModel, Field

import re

class PIIRedactor:
    def __init__(self):
        self.patterns = {
            "ssn": [
                re.compile(r"\b([0-3]{1}[0-9]{1}[0-1]{1}[0-9]{1}[0-9]{2})([-+A]{1})([0-9]{3})([a-zA-Z0-9]{1})\b"),
                re.compile(r"\b([0-3]{1}[0-9]{1}[0-1]{1}[0-9]{1}[0-9]{2})([-+A]{1})"),
                re.compile(r"\b([0-3]{1}[0-9]{1}[0-1]{1}[0-9]{1}[0-9]{2})([-+A]{1})?([a-zA-Z]{3,4})?\b"),
                re.compile(r"\b([0-3]{1}[0-9]{1}[0-1]{1}[0-9]{1}[0-9]{2})([-+A]{1})?([0-9]{3})?([a-zA-Z0-9]{1})?\b")
            ],
            # "phone": [
            #     re.compile(r"\b(\+?[0-9]{11,12})\b"),
            #     re.compile(r"\b(\+?[0-9]{2,3}\s?[0-9]{2,3}\s?[0-9]{1,3}\s?[0-9]{3}\s?[0-9]{4})\b"),
            #     re.compile(r"\b([0-9]{2,3}\s?[0-9]{1,3}\s?[0-9]{3,4}\s?[0-9]{3,4})\b"),
            #     re.compile(r"\b([0-9]{2,3}\s?[0-9]{3,4}\s?[0-9]{3,4})\b"),
            #     re.compile(r"\b([0-9]{2,3}\s?[0-9]{3,5}\s?[0-9]{3,5})\b"),
            #     re.compile(r"\b(\(?[0-9]{2,3}\)?\s?[0-9]{5,6}\)?)\b")
            # ]
        }

    def redact(self, text: str) -> str:
        for pattern in self.patterns["ssn"]:
            text = pattern.sub("[HETU]", text)
        # for pattern in self.patterns["phone"]:
        #     text = pattern.sub("[PUHELIN]", text)
        return text

class Filter:
    class Valves(BaseModel):
        # Regex settings
        redact_enabled: bool = Field(default=True, description="Redact PII")

    def __init__(self):
        self.file_handler = False
        self.valves = self.Valves()
        self._redactor = None

    @property
    def redactor(self):
        if self._redactor is None:
            self._redactor = PIIRedactor()
        return self._redactor

    def redact(self, text: str) -> str:
        if self.valves.redact_enabled:
            text = self.redactor.redact(text)
        return text

    def inlet(self, body: dict, __user__: Optional[dict] = None) -> dict:
        messages = body.get("messages", [])
        for message in messages:
            if message.get("role") == "user":
                content = message["content"]
                content = self.redact(content)
                message["content"] = content

        return body

    def outlet(self, body: dict, __user__: Optional[dict] = None) -> dict:
        return body