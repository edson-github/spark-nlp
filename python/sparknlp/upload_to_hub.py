import requests
import json
from typing import List
import sparknlp
import os
import zipfile


class PushToHub:
    list_of_tasks = [  # List  of available tasks in Modelhub
        "Named Entity Recognition",
        'Text Classification',
        'Text Generation',
        'Sentiment Analysis',
        'Translation',
        'Question Answering',
        'Summarization',
        'Sentence Detection',
        'Embeddings',
        'Language Detection',
        'Stop Words Removal',
        'Word Segmentation',
        'Part of Speech Tagging',
        'Lemmatization',
        'Chunk Mapping',
        'Spell Check',
        'Dependency Parser',
        'Pipeline Public']

    def zip_directory(self, zip_path: str):
        """Zips folder for pushing to hub.

        folder_path:Path to the folder to zip.
        zip_path:Path of the zip file to create."""

        with zipfile.ZipFile(zip_path, mode='w') as zipf:
            len_dir_path = len(self)
            for root, _, files in os.walk(self):
                for file in files:
                    file_path = os.path.join(root, file)
                    zipf.write(file_path, file_path[len_dir_path:])

    def unzip_directory(self):
        """Unzips Model to check for required files for upload.

        Keyword Arguments:
        zip_path:Zip Path to unzip.
        """

    def check_for_required_info(self):
        """Checks if the required fields exist in given dictionary  and fills any remaining fields.

        Keyword Arguments: 
        model_data: The model data to check.
        """
        
        list_of_required_fields = ['name', 'task', 'language', 'pythonCode', 'model_zip_path']

        if self['task'] not in PushToHub.list_of_tasks:
            list_of_tasks_string_version = "\n".join(PushToHub.list_of_tasks)
            raise ValueError(
                f"""Invalid task, please pick one of the following tasks\n{list_of_tasks_string_version}""")

        if self['model_zip_path'].endswith(".zip"):
            with zipfile.ZipFile(self['model_zip_path']) as modelfile:
                if 'metadata/part-00000' not in modelfile.namelist():
                    raise ValueError("The Model is not a Spark Saved Model.")
        elif not os.path.exists(f"{self['model_zip_path']}/metadata/part-00000"):
            raise ValueError("The Model is not a Spark Saved Model.")

    def push_to_hub(self, language: str, model_zip_path: str, task: str, pythonCode: str, GIT_TOKEN: str, title: str = None, tags: List[str] = None, dependencies: str = None, description: str = None, predictedEntities: str = None, sparknlpVersion: str = None, howToUse: str = None, liveDemo: str = None, runInColab: str = None, scalaCode: str = None, nluCode: str = None, results: str = None, dataSource: str = None, includedModels: str = None, benchmarking: str = None) -> str:
        """Pushes model to Hub.

        Keyword Arguments:
        model_data:Dictionary containing info about the model such as Name and Language.
        GIT_TOKEN: Token required for pushing to hub.
        """

        model_data = {item: value for (item, value) in locals().items() if value is not None}
        PushToHub.check_for_required_info(model_data)
        model_data = PushToHub.create_docs(model_data)

        r1 = requests.post('https://modelshub.johnsnowlabs.com/api/v1/models', data=json.dumps(model_data), headers={
            'Content-type': 'application/json',
            'Authorization': f'Bearer {GIT_TOKEN}'
        })

        if r1.status_code == 201:
            r2 = requests.post(
                f"https://modelshub.johnsnowlabs.com/api/v1/models/{r1.json()['id']}/file",
                data=open(model_data['model_zip_path'], 'rb'),
                headers={'Authorization': f'Bearer {GIT_TOKEN}'},
            )
            if r2.status_code == 200:
                print(r2.json()['message'])
                return r2.json()['message']
        else:
            print(f"Something Went Wrong During the Upload. Got Status Code: {r1.status_code}")
            return f"Something Went Wrong During the Upload. Got Status Code: {r1.status_code}"

    def create_docs(self) -> dict:
        """Adds fields in the dictionary for pushing to hub.

        Keyword Arguments:
        dictionary_for_upload: The dictionary to add keys to.
        """

        self['sparkVersion'] = "3.0"
        self['license'] = 'Open Source'
        self['supported'] = False

        if 'sparknlpVersion' not in self.keys():
            self['sparknlpVersion'] = f"Spark NLP {sparknlp.version()}"

        if 'description' not in self.keys():
            self[
                'description'
            ] = f"This model is used for {self['task']} and this model works with {self['language']} language"

        if 'title' not in self.keys():
            self['title'] = f"{self['task']} for {self['language']} language"

        if os.path.isdir(self['model_zip_path']):
            PushToHub.zip_directory(
                self['model_zip_path'], f"{self['model_zip_path']}.zip"
            )
            self['model_zip_path'] = self['model_zip_path'] + '.zip'
        return self
