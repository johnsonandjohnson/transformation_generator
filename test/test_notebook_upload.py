import unittest
import os

from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'Resources', '.env'))

class TestNotebookUpload(unittest.TestCase):
    generated_notebooks_dir = os.environ.get('GENERATED_NOTEBOOKS_DIR')
    databricks_instance = os.environ.get('DATABRICKS_INSTANCE')
    databricks_token = os.environ.get('DATABRICKS_PERSONAL_TOKEN')
    databricks_upload_dir = os.environ.get('DATABRICKS_UPLOAD_DIR')

    def _get_notebooks(self):
        list_of_notebooks = []
        for root, dirs, files in os.walk(self.generated_notebooks_dir):
            for file in files:
                list_of_notebooks.append(os.path.join(root, file))
        return list_of_notebooks

    def test_import_request(self):
        pass

    def test_base64_encode(self):
        pass

    def test_azure_import_response(self):
        pass

    def test_raised_import_response(self):
        pass


