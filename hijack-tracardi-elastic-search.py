import json

from tracardi.domain.resources.elastic_resource_config import ElasticResourceConfig, ElasticCredentials
from tracardi.service.plugin.domain.register import Plugin, Spec, MetaData, Documentation, PortDoc, Form, FormGroup, \
    FormField, FormComponent
from tracardi.service.plugin.runner import ActionRunner
from tracardi.service.plugin.domain.result import Result
from tracardi.service.storage.elastic_client import ElasticClient
from tracardi.process_engine.action.v1.connectors.elasticsearch.query.model.config import Config
from elasticsearch import ElasticsearchException
from tracardi.service.storage.driver.elastic import resource as resource_db
from tracardi.service.plugin.plugin_endpoint import PluginEndpoint


class ElasticSearchFetcher(ActionRunner):
    _client: ElasticClient
    config: Config
    
    async def set_up(self, init):
        config = Config(**init)
        resource = await resource_db.load(config.source.id)
        
        self.config = config
        credentials = resource.credentials.get_credentials(self, ElasticCredentials)
        self._client = credentials.get_client()
    
    async def run(self, payload: dict, in_edge=None) -> Result:
        
        try:
            query = json.loads(self.config.query)
            
            if 'size' not in query:
                query["size"] = 20
            
            if query["size"] > 50:
                self.console.warning("Fetching more then 50 records may impact the GUI performance.")
            
            result = await self._client.search(
                    index=self.config.index.id,
                    query=query
                    )
            
            await self._client.close()
        
        except ElasticsearchException as e:
            self.console.error(str(e))
            return Result(
                    port="error", value={
                        "message": str(e)
                        }
                    )
        
        return Result(port="result", value=result)

# if __name__ == '__main__':
    # ElasticSearchFetcher.