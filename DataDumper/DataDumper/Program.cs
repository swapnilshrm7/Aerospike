using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Aerospike.Client;
using CsvHelper;

namespace DataDumper
{
    class Program
    {
        static void Main(string[] args)
        {
            StreamReader reader = new StreamReader(@"C:\tweets1.csv");
            CsvReader csvreader = new CsvReader(reader);
            IEnumerable<TweetData> record = csvreader.GetRecords<TweetData>();
            var aerospikeClient = new AerospikeClient("18.235.70.103", 3000);
            string nameSpace = "AirEngine";
            string setName = "Swapnil";
            int count = 0;
            foreach (var item in record)
            {
                if (count <= 20000)
                {
                    if (count != 0)
                    {
                        var key = new Key(nameSpace, setName, item.id);
                        aerospikeClient.Put(new WritePolicy(), key, new Bin[] {
                         new Bin("text", item.text),
                         new Bin("favorited",item.favorited),
                         new Bin("favoriteCount",item.favoriteCount),
                         new Bin("created",item.created),
                         new Bin("truncated",item.truncated),
                         new Bin("id",item.id),
                         new Bin("statusSource",item.statusSource),
                         new Bin("screenName",item.screenName),
                         new Bin("retweetCount",item.retweetCount),
                         new Bin("isRetweet",item.isRetweet),
                         new Bin("timestamp",item.timestamp),
                         new Bin("date",item.date)
                        });
                    }
                    count++;
                }
                else
                {
                    break;
                }
            }
        reader.Close();
        }
    }
}
