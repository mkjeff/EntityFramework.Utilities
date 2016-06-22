using System;
using System.Linq.Expressions;

namespace EntityFramework.Utilities
{
    public class IdentitySpecification<T>
    {
        /// <summary>
        /// Set each column you use to identity.
        /// </summary>
        /// <param name="properties"></param>
        /// <returns></returns>
        public IdentitySpecification<T> ColumnsToIdentity(params Expression<Func<T, object>>[] properties)
        {
            Properties = properties;
            return this;
        }

        public Expression<Func<T, object>>[] Properties { get; set; }
    }

}