using ElasticProject.Data;
using ElasticProject.Data.Interface;
using Microsoft.AspNetCore.Mvc;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Threading.Tasks;
using System.Text.Json;

namespace ElasticProject.Api.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class HomeController : ControllerBase
    {
        private IElasticsearchService _elasticsearchService;
        public HomeController(IElasticsearchService elasticsearchService)
        {
            _elasticsearchService = elasticsearchService;
        }
   
        [HttpPost("insert-all-cities")]
        public async Task<IActionResult> InsertAllCities()
        {
            var responseJson = ""; 
            using (var client = new HttpClient())
            {
                // API URI
                client.BaseAddress = new Uri("http://localhost:5181/");
               
               var response = await client.GetAsync(client.BaseAddress+ "api/Cities/get-all-cities");
               responseJson = await response.Content.ReadAsStringAsync();
              
            }
            var responsed = JsonConvert.DeserializeObject<List<Cities>>(responseJson);
            await _elasticsearchService.InsertBulkDocuments("cities", responsed);
            return Ok("");
        }
        [HttpPut("add-city/{cityName}")]
        public async Task<IActionResult> AddCity(string cityName)
        {
            Cities cities = new Cities
            {
                City = cityName,
                CreateDate = System.DateTime.Now,
                Id = Guid.NewGuid().ToString(),
                Population = 0,
                Region = "Default"
            };
            await _elasticsearchService.InsertDocument("cities", cities);
            return Ok("");
        }
        [HttpGet("get-city/{id}")]
        public async Task<IActionResult> GetCityById(string id)
        {
            Cities getCities = await _elasticsearchService.GetDocument("cities", id);
            return Ok(getCities);
        }
        [HttpDelete("delete-city/{id}")]
        public async Task<IActionResult> DeleteCityById(string id)
        {
            Cities getCities = await _elasticsearchService.GetDocument("cities", id);
            await _elasticsearchService.DeleteByIdDocument("cities", new Cities { Id = id });


            return Ok($" {id} city is deleted. City name was : {getCities.City}");
        }
        [HttpGet("get-all-cities")]
        public async Task<IActionResult> GetAllCities()
        {
            List<Cities> getCities = await _elasticsearchService.GetDocuments("cities");
            return Ok(getCities);
        }
        [HttpGet("get-all-regions")]
        public async Task<IActionResult> GetAllRegions()
        {
            List<Cities> getRegions = await _elasticsearchService.GetDocuments("regions");

            return Ok(getRegions);
        }
        [HttpDelete("delete-all-cities")]
        public async Task<IActionResult> DeleteAllCities()
        {
            await _elasticsearchService.DeleteIndex("cities");
            return Ok($"cities index deleted.");
        }
        [HttpGet("get-cities-by-region/{regionName}")]
        public async Task<IActionResult> GetCitiesByRegion(string regionName)
        {
            List<Cities> cities =  await _elasticsearchService.GetDocumentsByRegion("cities", regionName);
            return Ok(cities);
        }
    }
}
