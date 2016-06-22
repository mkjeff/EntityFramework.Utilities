using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;

namespace EntityFramework.Utilities
{
    public class TSQLGenerator
    {
        public virtual string BuildDropStatement(string schema, string tempTableName)
        {
            return $"DROP table {schema}.[{tempTableName}]";
        }

        public virtual string BuildMergeCommand(string tableName, IList<ColumnMapping> properties, string tempTableName)
        {
            var setters = string.Join(",", properties.Where(c => !c.IsPrimaryKey).Select(c => "[" + c.NameInDatabase + "] = TEMP.[" + c.NameInDatabase + "]"));
            var pks = properties.Where(p => p.IsPrimaryKey).Select(x => "ORIG.[" + x.NameInDatabase + "] = TEMP.[" + x.NameInDatabase + "]");
            var filter = string.Join(" and ", pks);
            var mergeCommand = $@"UPDATE [{tableName}]
                SET
                    {setters}
                FROM
                    [{tableName}] ORIG
                INNER JOIN
                     [{tempTableName}] TEMP
                ON 
                    {filter}";

            return mergeCommand;
        }

        public virtual string BuildSelectIntoCommand(string tableName, IList<ColumnMapping> properties, string tempTableName)
        {
            var output = properties.Where(p => p.IsStoreGenerated).Select(x => "INSERTED.[" + x.NameInDatabase + "]");
            var mergeCommand = $@"INSERT INTO [{tableName}]
                OUTPUT {string.Join(", ", output)}
                SELECT * FROM 
                    [{tempTableName}]";
            return mergeCommand;
        }

        


        public virtual string BuildDeleteQuery(QueryInformation queryInfo)
        {
            return string.Format($"DELETE FROM [{ queryInfo.Schema}].[{queryInfo.Table}] {queryInfo.WhereSql}");
        }

        public virtual string BuildUpdateQuery(QueryInformation predicateQueryInfo, QueryInformation modificationQueryInfo)
        {
            var msql = modificationQueryInfo.WhereSql.Replace("WHERE ", "");
            var indexOfAnd = msql.IndexOf("AND", StringComparison.Ordinal);
            var update = indexOfAnd == -1 ? msql : msql.Substring(0, indexOfAnd).Trim();

            var updateRegex = new Regex(@"(\[[^\]]+\])[^=]+=(.+)", RegexOptions.IgnoreCase);
            var match = updateRegex.Match(update);
            string updateSql;
            if (match.Success)
            {
                var col = match.Groups[1];
                var rest = match.Groups[2].Value;

                rest = SqlStringHelper.FixParantheses(rest);

                updateSql = col.Value + " = " + rest;
            }
            else
            {
                updateSql = string.Join(" = ", update.Split(new string[] { " = " }, StringSplitOptions.RemoveEmptyEntries).Reverse());
            }


            return $"UPDATE [{predicateQueryInfo.Schema}].[{predicateQueryInfo.Table}] SET {updateSql} {predicateQueryInfo.WhereSql}";
        }


        public virtual string BuildMergeIntoCommand(string tableName, IList<ColumnMapping> properties, string tempTableName,HashSet<string> columnsToIdentity,HashSet<string> columnsToUpdate)
        {
            var insertProperties = properties.Where(p => !p.IsStoreGenerated).Select(p => p.NameInDatabase).ToArray();

            string mergeCommand = 
                $@"merge into [{tableName}] as Target 
	 using {tempTableName} as Source 
	 	on {string.Join(" and ", properties
         .Where(p => columnsToIdentity.Contains(p.NameOnObject))
         .Select(p => $"Target.{p.NameInDatabase}=Source.{p.NameInDatabase}"))}            
	 when matched then 
	 update set {string.Join(",", properties
     .Where(p => columnsToUpdate.Contains(p.NameOnObject) && !p.IsPrimaryKey)
     .Select(p => "Target.[" + p.NameInDatabase + "] = Source.[" + p.NameInDatabase + "]"))}
	 when not matched then 
	 insert (
	  {string.Join(",", insertProperties)}
	 ) values (
      {string.Join(",", insertProperties.Select(p => $"Source.{p}"))}
	);";
            return mergeCommand;
        }
    }
}
