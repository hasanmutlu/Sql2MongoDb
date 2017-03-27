using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Dynamic;
using MongoDB.Driver;
using MongoDB.Bson;

namespace Sql2MongoDb
{
    public class Sql2MongoDbConverter
    {
        private static SqlConnection _sqlConnection;
        private static MongoClient _mongoClient;
        public event Action<Exception> OnError;

        public Sql2MongoDbConverter(SqlConnection sqlConnection, MongoClient mongoClient)
        {
            _sqlConnection = sqlConnection;
            _mongoClient = mongoClient;
        }

        public void ConvertDataBase(string sqlDatabaseName, string mongoDatabaseName = null)
        {
            try
            {
                mongoDatabaseName = string.IsNullOrEmpty(mongoDatabaseName) ? sqlDatabaseName : mongoDatabaseName;
                _sqlConnection.Open();
                var db = _mongoClient.GetDatabase(mongoDatabaseName);
                var tableList = GetTablesOfDatabase(sqlDatabaseName);
                foreach (var table in tableList)
                {
                    var collection = db.GetCollection<dynamic>(table.Key);
                    ConvertTable(collection,new MongoConverterOptions {SqlTableName = table.Value});
                }
                _sqlConnection.Close();

            }
            catch (Exception ex)
            {
                NotifyError(ex);
            }
        }
        private Dictionary<string, int> GetTableFields(IDataRecord dataReader)
        {
            var result = new Dictionary<string, int>();
            for (var i = 0; i < dataReader.FieldCount; i++)
            {
                var fieldName = dataReader.GetName(i);
                result.Add(fieldName, i);
            }
            return result;
        }

        private Dictionary<string, string> GetTablesOfDatabase(string databaseName)
        {
            var result = new Dictionary<string, string>();
            var cmd = new SqlCommand($"SELECT TABLE_SCHEMA,TABLE_NAME FROM {databaseName}.INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE'", _sqlConnection);
            var reader = cmd.ExecuteReader();
            if (reader.HasRows)
            {
                while (reader.Read())
                {
                    var tableName = $"{databaseName}.{reader.GetString(0)}.{reader.GetString(1)}";
                    result.Add(reader.GetString(1), tableName);
                }
            }
            return result;
        }

        public void ConvertTable(MongoConverterOptions options)
        {
            try
            {
                _sqlConnection.Open();
                var db = _mongoClient.GetDatabase(options.MongoDataBaseName);
                var collection = db.GetCollection<dynamic>(options.MongoCollectionName);
                if (!string.IsNullOrEmpty(options.MongoCollectionIndex))
                {
                    var index = Builders<dynamic>.IndexKeys.Ascending(options.MongoCollectionIndex);
                    collection.Indexes.CreateOne(index);
                }
                ConvertTable(collection,options);
                _sqlConnection.Close();
                
            }
            catch (Exception ex)
            {
                NotifyError(ex);
            }
        }

        private void ConvertTable(IMongoCollection<dynamic> collection, MongoConverterOptions options)
        {
                var selectCommand = new SqlCommand($"SELECT * FROM {options.SqlTableName};", _sqlConnection);
                var dataReader = selectCommand.ExecuteReader();
                if (!dataReader.HasRows)
                {
                    return;
                }
                var tableFields = GetTableFields(dataReader);
                while (dataReader.Read())
                {
                    var row = GetRowData(dataReader, tableFields);
                    if (options.FilterProcess != null)
                    {
                            if (!options.FilterProcess(row))
                            {
                                continue;
                            }
                    }
                    if (options.PostProcess != null)
                    {
                            row = options.PostProcess(row);
                    }
                    collection.InsertOne(row);
                }
                selectCommand.Dispose();
                dataReader.Dispose();
        }

        private dynamic GetRowData(IDataRecord dataReader, Dictionary<string, int> tableFields)
        {
            var result = new ExpandoObject();
            var resultContainer = ((IDictionary<string, object>)result);
            foreach (var item in tableFields)
            {
                var value = dataReader.GetValue(item.Value);
                value = value is DBNull ? null : value;
                resultContainer.Add(item.Key, value);
            }
            return result;
        }

        private void NotifyError(Exception ex)
        {
            OnError?.Invoke(ex);
        }
    }
}
