using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Dynamic;
using System.Linq;
using MongoDB.Driver;

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

        public void ConvertTable<T>(string sqlTableName,string mongoDataBaseName,string mongoCollectionName=null) where T:new()
        {
            try
            {
                mongoCollectionName = string.IsNullOrEmpty(mongoCollectionName)? sqlTableName.Split('.').Last(): mongoCollectionName;
                _sqlConnection.Open();
                var db = _mongoClient.GetDatabase(mongoDataBaseName);
                var collection = db.GetCollection<T>(mongoCollectionName);
                ConvertTable(collection,sqlTableName);
                _sqlConnection.Close();
                _sqlConnection.Dispose();
                
            }
            catch (Exception ex)
            {
                NotifyError(ex);
            }
        }

        public void ConvertDataBase(string sqlDatabaseName,string mongoDatabaseName=null)
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
                    ConvertTable(collection, table.Value);
                }
                _sqlConnection.Close();
                _sqlConnection.Dispose();

            }
            catch (Exception ex)
            {
                NotifyError(ex);
            }
        }

        private void ConvertTable<T>(IMongoCollection<T> collection, string sqlTableName) where T:new()
        {
                var selectCommand = new SqlCommand($"SELECT * FROM {sqlTableName};", _sqlConnection);
                var dataReader = selectCommand.ExecuteReader();
                if (!dataReader.HasRows)
                {
                    return;
                }
                var tableFields = GetTableFields(dataReader);
                while (dataReader.Read())
                {
                    var row = GetRowData<T>(dataReader, tableFields);
                    collection.InsertOne(row);
                }
                selectCommand.Dispose();
                dataReader.Dispose();
        }
        private Dictionary<string,string> GetTablesOfDatabase(string databaseName)
        {
            var result = new Dictionary<string, string>();
            var cmd = new SqlCommand($"SELECT TABLE_SCHEMA,TABLE_NAME FROM {databaseName}.INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE'", _sqlConnection);
            var reader = cmd.ExecuteReader();
            if (reader.HasRows)
            {
                while (reader.Read())
                {
                    var tableName = $"{databaseName}.{reader.GetString(0)}.{reader.GetString(1)}";
                    result.Add(reader.GetString(1),tableName);
                }
            }
            return result;
        }


        private T GetRowData<T>(IDataRecord dataReader, Dictionary<string, int> tableFields) where T:new()
        {
            dynamic result = new ExpandoObject();
            var resultContainer = ((IDictionary<string, object>)result);
            foreach (var item in tableFields)
            {
                var value = dataReader.GetValue(item.Value);
                value = value is DBNull ? null : value;
                resultContainer.Add(item.Key, value);
            }
            return (T)result;
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

        private void NotifyError(Exception ex)
        {
            OnError?.Invoke(ex);
        }
    }
}
