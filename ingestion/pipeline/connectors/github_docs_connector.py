from pipeline.connectors.base_connector import BaseConnector

class GithubDocsConnector(BaseConnector):
    def __init__(self, target_version, repo_url, docs_path="docs", file_name="index.md"):
        super().__init__(target_version)
        self.repo_url = repo_url
        self.docs_path = docs_path
        self.file_name = file_name

    def get_documentation_url(self, version):
        return f"{self.repo_url}/tree/v{version}/{self.docs_path}/{self.file_name}"