using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Sql2MongoDb
{
    public class MongoConverterOptions
    {
        private string _mongoCollectionName;
        public string SqlTableName { get; set; }
        public string MongoDataBaseName {
            get { return _mongoCollectionName; }
            set
            {
                _mongoCollectionName = string.IsNullOrEmpty(value) ? value.Split('.').Last() : value;
            }
        }
        public string MongoCollectionName { get; set; }
        public string MongoCollectionIndex { get; set; }
        public Func<dynamic, object> PostProcess { get; set; }
        public Func<dynamic, bool> FilterProcess { get; set; }


    }
}
