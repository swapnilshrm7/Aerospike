using Aerospike.Client;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Web.Http;

namespace AerospikeCRUD.Controllers
{
    public class AerospikeController : ApiController
    {

        AerospikeClient aerospikeClient = new AerospikeClient("18.235.70.103", 3000);
        string nameSpace = "AirEngine";
        string setName = "Swapnil"; 
         
        [HttpPut]
        [Route("GetDataById")]
        public List<Record> GetDataById([FromBody]List<string> input)
        {
            List<Record> output = new List<Record>();
            foreach (var id in input)
            {
                var key = new Key(nameSpace, setName, id.ToString());
                Record dataById = aerospikeClient.Get(new WritePolicy(), key);
                output.Add(dataById);
            }
            return output;
        }

        public void Delete([FromBody]string tweetId)
        {
            aerospikeClient.Delete(new WritePolicy(), new Key(nameSpace, setName, tweetId));
        }

        // Put: api/Trolls/ Updataing content of a tweet using their tweet id.
        public void Put([FromBody]Tweet tweet)
        {
            aerospikeClient.Put(new WritePolicy(), new Key(nameSpace, setName, tweet.id), new Bin[] { new Bin(""+tweet.binName, tweet.newValue) });
        }
    }
}
