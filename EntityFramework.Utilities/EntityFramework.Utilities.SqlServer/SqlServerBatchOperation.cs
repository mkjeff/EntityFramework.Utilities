using System;
using System.Collections.Generic;
using System.Data.Entity;
using System.Data.Entity.Core.EntityClient;
using System.Data.Entity.Core.Objects;
using System.Data.Entity.Infrastructure;
using System.Data.SqlClient;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;

namespace EntityFramework.Utilities.SqlServer
{
    public interface ISqlServerBatchOperationBase<TContext, T> where T : class
    {
        ISqlServerBatchOperationFiltered<TContext, T> Where(Expression<Func<T, bool>> predicate);

        /// <summary>
        /// Bulk insert all items if the Provider supports it. Otherwise it will use the default insert unless Configuration.DisableDefaultFallback is set to true in which case it would throw an exception.
        /// </summary>
        /// <param name="items">The items to insert</param>     
        Task InsertAllAsync<TEntity>(IEnumerable<TEntity> items, SqlServerBulkSettings settings = null) where TEntity : class, T;

        /// <summary>
        /// Bulk update all items if the Provider supports it. Otherwise it will use the default update unless Configuration.DisableDefaultFallback is set to true in which case it would throw an exception.
        /// </summary>
        /// <typeparam name="TEntity"></typeparam>
        /// <param name="items">The items to update</param>
        Task UpdateAllAsync<TEntity>(IEnumerable<TEntity> items, Action<UpdateSpecification<TEntity>> updateSpecification, SqlServerBulkSettings settings = null) where TEntity : class, T;

        /// <summary>
        /// provider batch upsert operation
        /// SQL:
        /// merge into [(the table of source entity)] as Target 
        /// using (tempTable) as Source
        ///     on <paramref name="identitySpecification"/>
        /// when matched then
        ///     update set <paramref name="whenMatchedUpdateSpecification"/>
	    /// when not matched then
        ///     insert ...;
        /// </summary>
        /// <typeparam name="TEntity"></typeparam>
        /// <param name="items">The items to upsert</param>
        /// <param name="identitySpecification">match identity specification. if parameter is null, use primary key as default</param>
        /// <param name="whenMatchedUpdateSpecification">update specification when matched by <paramref name="identitySpecification"/>. if parameter is null, update all columns except primary key</param>
        Task MergeAllAsync<TEntity>(IEnumerable<TEntity> items, Action<IdentitySpecification<TEntity>> identitySpecification = null, Action<UpdateSpecification<TEntity>> whenMatchedUpdateSpecification = null, SqlServerBulkSettings settings = null) where TEntity : class, T;
    }

    public interface ISqlServerBatchOperationFiltered<TContext, T>
    {
        Task<int> DeleteAsync(SqlServerDeleteSettings settings = null);
        Task<int> UpdateAsync<TP>(Expression<Func<T, TP>> prop, Expression<Func<T, TP>> modifier, SqlServerUpdateSettings settings = null);
    }

    public class SqlServerBatchOperation<TContext, T> : ISqlServerBatchOperationBase<TContext, T>, ISqlServerBatchOperationFiltered<TContext, T>
        where T : class
        where TContext : DbContext
    {
        private ObjectContext context;
        private DbContext dbContext;
        private IDbSet<T> set;
        private Expression<Func<T, bool>> predicate;

        internal SqlServerBatchOperation(TContext context, IDbSet<T> set)
        {
            this.dbContext = context;
            this.context = (context as IObjectContextAdapter).ObjectContext;
            this.set = set;
        }

        public static ISqlServerBatchOperationBase<TContext, T> For(TContext context, IDbSet<T> set)
        {
            return new SqlServerBatchOperation<TContext, T>(context, set);
        }

        private SqlConnection GetConnectionOrThrow(SqlConnection manualConnection = null)
        {
            var con = (context.Connection as EntityConnection)?.StoreConnection as SqlConnection;
            if (con == null && manualConnection == null)
            {
                throw new InvalidOperationException("No connection that can be used was found. This is usually because the connection was wrapped with for example a profiler. If so you need to supply an SqlConnection as part of the settings");
            }

            return manualConnection ?? con;
        }

        /// <summary>
        /// Bulk insert all items if the Provider supports it. Otherwise it will use the default insert unless Configuration.DisableDefaultFallback is set to true in which case it would throw an exception.
        /// </summary>
        /// <param name="items">The items to insert</param>
        public async Task InsertAllAsync<TEntity>(IEnumerable<TEntity> items, SqlServerBulkSettings settings = null) where TEntity : class, T
        {
            settings = settings ?? new SqlServerBulkSettings();
            var connectionToUse = GetConnectionOrThrow();
            settings.Connection = connectionToUse;
            settings.TempSettings = new TempTableSqlServerBulkSettings(settings);

            var tableSpec = BulkTableSpec.Get<TEntity, T>(this.dbContext);

            await settings.Factory.Inserter().InsertItemsAsync(items, tableSpec, settings);
        }


        public async Task UpdateAllAsync<TEntity>(IEnumerable<TEntity> items, Action<UpdateSpecification<TEntity>> updateSpecification, SqlServerBulkSettings settings = null) where TEntity : class, T
        {

            settings = settings ?? new SqlServerBulkSettings();
            var connectionToUse = GetConnectionOrThrow();
            settings.Connection = connectionToUse;
            settings.TempSettings = new TempTableSqlServerBulkSettings(settings);

            var tableSpec = BulkTableSpec.Get<TEntity, T>(this.dbContext);

            var spec = new UpdateSpecification<TEntity>();
            updateSpecification(spec);
            await settings.Factory.Inserter().UpdateItemsAsync(items, tableSpec, settings, spec);

        }

        public ISqlServerBatchOperationFiltered<TContext, T> Where(Expression<Func<T, bool>> predicate)
        {
            this.predicate = predicate;
            return this;
        }

        public async Task<int> DeleteAsync(SqlServerDeleteSettings settings = null)
        {
            settings = settings ?? new SqlServerDeleteSettings();
            var set = context.CreateObjectSet<T>();
            var query = (ObjectQuery<T>)set.Where(this.predicate);
            var queryInformation = settings.Analyzer.Analyze(query);

            var delete = settings.SqlGenerator.BuildDeleteQuery(queryInformation);
            var parameters = query.Parameters.Select(p => new SqlParameter { Value = p.Value, ParameterName = p.Name }).ToArray<object>();
            return await context.ExecuteStoreCommandAsync(delete, parameters);
        }

        public async Task<int> UpdateAsync<TP>(Expression<Func<T, TP>> prop, Expression<Func<T, TP>> modifier, SqlServerUpdateSettings settings = null)
        {
            settings = settings ?? new SqlServerUpdateSettings();
            var set = context.CreateObjectSet<T>();

            var query = (ObjectQuery<T>)set.Where(predicate);
            var queryInformation = settings.Analyzer.Analyze(query);

            var updateExpression = ExpressionHelper.CombineExpressions(prop, modifier);

            var mquery = ((ObjectQuery<T>)context.CreateObjectSet<T>().Where(updateExpression));
            var mqueryInfo = settings.Analyzer.Analyze(mquery);

            var update = settings.SqlGenerator.BuildUpdateQuery(queryInformation, mqueryInfo);

            var parameters = query.Parameters
                .Concat(mquery.Parameters)
                .Select(p => new SqlParameter { Value = p.Value, ParameterName = p.Name })
                .ToArray<object>();

            return await context.ExecuteStoreCommandAsync(update, parameters);
        }

        public async Task MergeAllAsync<TEntity>(IEnumerable<TEntity> items, Action<IdentitySpecification<TEntity>> identitySpecification, Action<UpdateSpecification<TEntity>> whenMatchedUpdateSpecification, SqlServerBulkSettings settings) where TEntity : class, T
        {
            settings = settings ?? new SqlServerBulkSettings();
            var connectionToUse = GetConnectionOrThrow();
            settings.Connection = connectionToUse;
            settings.TempSettings = new TempTableSqlServerBulkSettings(settings);

            var tableSpec = BulkTableSpec.Get<TEntity, T>(this.dbContext);


            await settings.Factory.Inserter().UpsertItemsAsync(items, tableSpec, settings, identitySpecification, whenMatchedUpdateSpecification);
        }
    }
}
