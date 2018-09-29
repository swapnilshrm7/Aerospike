using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataDumper
{
    class TweetData
    {
        public string text { get; set; }
        public string favorited { get; set; }
        public string favoriteCount { get; set; }
        public string created { get; set; }
        public string truncated { get; set; }
        public string id { get; set; }
        public string statusSource { get; set; }
        public string screenName { get; set; }
        public string retweetCount { get; set; }
        public string isRetweet { get; set; }
        public string timestamp { get; set; }
        public string date { get; set; }
    }
}
