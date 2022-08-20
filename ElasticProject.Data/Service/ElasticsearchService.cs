using ElasticProject.Data.Interface;
using Microsoft.Extensions.Configuration;
using Nest;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ElasticProject.Data
{
    public class ElasticsearchService : IElasticsearchService
    {
        private readonly IConfiguration _configuration;
        private readonly IElasticClient _client;
        public ElasticsearchService(IConfiguration configuration)
        {
            _configuration = configuration;
            _client = CreateInstance();
        }
        private ElasticClient CreateInstance()
        {
            string host = _configuration.GetSection("ElasticsearchServer:Host").Value;
            string port = _configuration.GetSection("ElasticsearchServer:Port").Value;
            string username = _configuration.GetSection("ElasticsearchServer:Username").Value;
            string password = _configuration.GetSection("ElasticsearchServer:Password").Value;
            var settings = new ConnectionSettings(new Uri(host + ":" + port));
            if (!string.IsNullOrEmpty(username) && !string.IsNullOrEmpty(password))
                settings.BasicAuthentication(username, password);

            return new ElasticClient(settings);
        }
        public async Task ChekIndex(string indexName)
        {
            var anyy = await _client.Indices.ExistsAsync(indexName);
            if (anyy.Exists)
                return;

            var response = await _client.Indices.CreateAsync(indexName,
                ci => ci
                    .Index(indexName)
                    .CitiesMapping()
                    .Settings(s => s.NumberOfShards(3).NumberOfReplicas(1))
                    );

            return;
        }
        public async Task DeleteIndex(string indexName)
        {
            await _client.Indices.DeleteAsync(indexName);
        }
        public async Task<Cities> GetDocument(string indexName, string id)
        {
            var response = await _client.GetAsync<Cities>(id, q => q.Index(indexName));
            return response.Source;
        }
        public async Task<List<Cities>> GetDocuments(string indexName)
        {
            var response = await _client.SearchAsync<Cities>(s => s
                             .Index(indexName)
                             .Size(100)
                          .Query(q => q
                      .MatchAll()
                     ));

            return response.Documents.ToList();
        }
        public async Task InsertDocument(string indexName, Cities cities)
        {
            var response = await _client.CreateAsync(cities, q => q.Index(indexName));
            if (response.ApiCall?.HttpStatusCode == 409)
            {
                await _client.UpdateAsync<Cities>(cities.Id, a => a.Index(indexName).Doc(cities));
            }
        }
        public async Task InsertBulkDocuments(string indexName, List<Cities> cities)
        {
            await _client.IndexManyAsync(cities, index: indexName);
        }
        public async Task DeleteByIdDocument(string indexName, Cities cities)
        {
            var response = await _client.CreateAsync(cities, q => q.Index(indexName));
            if (response.ApiCall?.HttpStatusCode == 409)
            {
                await _client.DeleteAsync(DocumentPath<Cities>.Id(cities.Id).Index(indexName));
            }
        }

        public async Task<List<Cities>> GetDocumentsByRegion(string indexName, string regionName)
        {
            var response = await _client.SearchAsync<Cities>(s => s
                            .Index(indexName)
                            .Size(100)
                         .Query(q => q
                    .Match(m => m.Field("region").Query(regionName)
                    )));

            return response.Documents.ToList();
        }


    }
}
