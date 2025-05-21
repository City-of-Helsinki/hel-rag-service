import os
from typing import List, Union, Generator, Iterator

from openai import OpenAI
from pydantic import BaseModel


class Pipeline:
    """
    This class represents an example pipeline for integrating with OpenAI's API.
    It demonstrates how to set up a Retrieval-Augmented Generation (RAG) pipeline
    that uses a knowledge base to provide context-aware responses to user queries.
    """

    class Config(BaseModel):
        """
        Configuration class for storing API-related settings.
        """
        OPENAI_API_KEY: str
        OPENAI_API_BASE_URL: str
        DEFAULT_MODEL: str

    def __init__(self):
        """
        Initializes the pipeline with default configurations and a static knowledge base.
        Sets up the OpenAI client for interacting with the API.
        """
        self.SYSTEM_MESSAGE_TEMPLATE = ("Vastaa käyttäjän kysymykseen hyödyntäen ohessa olevaa dataa. "
                                        "Jos konteksti ei sisällä vastausta, et saa vastata. "
                                        "Vinkkaa että käyttäjä voisi kysyä OpenWebUI pipelines laajennoksesta."
                                        "\n\n"
                                        "DATA: "
                                        ""
                                        "{context}")
        self.documents = None

        # Setup LLM client
        # Read environment variables or use default values
        # Save values to config object for later use
        self.config = self.Config(
            **{
                "OPENAI_API_KEY": os.getenv("OPENAI_API_KEY", "ANY_VALUE"),
                "OPENAI_API_BASE_URL": os.getenv("OPENAI_API_BASE_URL", "http://localhost:4000"),
                "DEFAULT_MODEL": os.getenv("DEFAULT_MODEL", "azure-gpt-4o"),
            }
        )

        # Setup LiteLLM client
        self.client = OpenAI(
            api_key=self.config.OPENAI_API_KEY,
            base_url=self.config.OPENAI_API_BASE_URL
        )

        # For RAG pipeline, you would typically load your knowledge base here
        # Note: this is just an example, you can use any other data source
        self.documents = ("""

            Pipelines | Open WebUI

            Pipelines on Open WebUI:n laajennos, joka mahdollistaa mukautettavat työnkulut ja monipuoliset integraatiot. Se mahdollistaa python kirjastojen käytön ja tarjoaa laajemmat mahdollisuudet luoda räätälöityjä työnkulkuja, jotka voivat sisältää monimutkaisempia laskentatehtäviä tai logiikkaa.

            Voit rakentaa omia pipelines laajennoksia luomalla custom_pipelines hakemiston alle oman toteutuksen. Katso esimerkki example_pipeline hakemistosta.

            Lisätietoa löydät: https://docs.openwebui.com/pipelines/
        """)

    def pipe(
            self, user_message: str, model_id: str, messages: List[dict], body: dict
    ) -> Union[str, Generator, Iterator]:
        """
        Processes a user message using the pipeline.

        This method demonstrates how to implement a basic RAG pipeline by:
        - Retrieving relevant context data from a knowledge base.
        - Formatting the system message with the retrieved context.
        - Sending the user message and system message to the OpenAI API for response generation.

        :param user_message: The message from the user.
        :param model_id: The model identifier (not used in this example).
        :param messages: A list of messages to be sent to the OpenAI API.
        :param body: Additional request body parameters (not used in this example).
        :return: The response from the OpenAI API or an error message.
        """
        print(f"User message: {user_message}")

        # Skip RAG if this is call for title generation
        # Title generation prompt by default starts with "Here is the query:" so this is not a perfect check
        if not user_message.startswith("Here is the query:"):
            # Basic RAG logic: query data using user message
            # and inject new system message with context data and custom instructions

            # Build context data using user input
            context_data = self._build_context_data(user_message)
            # Format system message content: combine template with context data
            system_message_content = self.SYSTEM_MESSAGE_TEMPLATE.format(context=context_data)
            # Build system message and inject it into the messages list
            system_message = {'content': system_message_content, 'role': "system"}
            # Append into messages list
            messages.append(system_message)
            print(f"System message: {system_message}")

        # Append user message to messages list
        messages.append({'content': user_message, 'role': "user"})

        # Then call LLM with the messages
        try:
            print(f"Calling LLM with messages: {messages}")
            response = self.client.chat.completions.create(model=self.config.DEFAULT_MODEL, messages=messages,
                                                           temperature=0.7)
            return response.choices[0].message.content
        except Exception as e:
            print(f"Error calling LLM: {e}")
            return "Tapahtui virhe!"

    def _build_context_data(self, user_message: str) -> str:
        """
        Builds context data for the RAG pipeline.

        This method simulates retrieving relevant information from a knowledge base
        or other data source based on the user message. In this example, it uses static data.

        :param user_message: The message from the user.
        :return: The context data as a string.
        """
        # Here we could query data from a database or other source to build context data
        # For demonstration purposes, we use a simple static data
        return self.documents


if __name__ == "__main__":
    """
    Helper function to test the pipeline without deploying it to the server.
    Install requirements: pip install openai pydantic
    """
    p = Pipeline()
    res1 = p.pipe("Kuinka voit?", "model_id", [], {})
    print(res1)
    res2 = p.pipe("Miten otan käyttöön pipelinen?", "model_id", [], {})
    print(res2)
    res3 = p.pipe("Mistä löydän lisätietoa?", "model_id", [], {})
    print(res3)