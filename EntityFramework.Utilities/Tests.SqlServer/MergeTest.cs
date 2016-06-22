using System.Collections.Generic;
using System.Data.Entity;
using System.Linq;
using System.Threading.Tasks;
using EntityFramework.Utilities;
using EntityFramework.Utilities.SqlServer;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Tests.FakeDomain;
using Tests.FakeDomain.Models;

namespace Tests
{
    [TestClass]
    public class MergeTest
    {
        [TestMethod]
        public async Task Merge_Default()
        {
            await Setup();

            using (var db = Context.Sql())
            {
                var posts = await db.BlogPosts.ToListAsync();
                foreach (var post in posts)
                {
                    post.Title = post.Title.Replace("1", "4").Replace("2", "8").Replace("3", "12");
                }
                var insert = BlogPost.Create("TNew");
                posts.Add(insert);
                await EFBatchOperation.For(db, db.BlogPosts).MergeAllAsync(posts);
            }

            using (var db = Context.Sql())
            {
                var posts = await db.BlogPosts.OrderBy(b => b.ID).ToListAsync();
                Assert.AreEqual("T4", posts[0].Title);
                Assert.AreEqual("T8", posts[1].Title);
                Assert.AreEqual("T12", posts[2].Title);
                Assert.AreEqual("TNew", posts[3].Title);
            }
        }

        [TestMethod]
        public async Task Merge_With_Condition()
        {
            await Setup();

            using (var db = Context.Sql())
            {
                var posts = await db.BlogPosts.ToListAsync();
                foreach (var post in posts)
                {
                    post.Title = post.Title.Replace("1", "4").Replace("2", "8").Replace("3", "12");
                }
                var insert = BlogPost.Create("TNew");
                posts.Add(insert);
                await EFBatchOperation.For(db, db.BlogPosts).MergeAllAsync(posts, c => c.ColumnsToIdentity(p => p.ID));
            }

            using (var db = Context.Sql())
            {
                var posts = await db.BlogPosts.OrderBy(b => b.ID).ToListAsync();
                Assert.AreEqual("T4", posts[0].Title);
                Assert.AreEqual("T8", posts[1].Title);
                Assert.AreEqual("T12", posts[2].Title);
                Assert.AreEqual("TNew", posts[3].Title);
            }
        }

        [TestMethod]
        public async Task Merge_With_Specific_Update_Column()
        {
            await Setup();

            using (var db = Context.Sql())
            {
                var posts = await db.BlogPosts.ToListAsync();
                foreach (var post in posts)
                {
                    post.Title = post.Title.Replace("1", "4").Replace("2", "8").Replace("3", "12");
                    post.Reads = 99;
                }
                var insert = BlogPost.Create("TNew");
                insert.Reads = 99;
                posts.Add(insert);
                await EFBatchOperation.For(db, db.BlogPosts).MergeAllAsync(posts, null,c=>c.ColumnsToUpdate(p=>p.Reads));
            }

            using (var db = Context.Sql())
            {
                var posts = await db.BlogPosts.OrderBy(b => b.ID).ToListAsync();
                posts.ForEach(p => Assert.AreEqual(99, p.Reads));
                Assert.AreEqual("T1", posts[0].Title);
                Assert.AreEqual("T2", posts[1].Title);
                Assert.AreEqual("T3", posts[2].Title);
                Assert.AreEqual("TNew", posts[3].Title);
            }
        }

        [TestMethod]
        public async Task Merge_With_Condition_And_Specific_Update_Column()
        {
            await Setup();

            using (var db = Context.Sql())
            {
                var posts = await db.BlogPosts.ToListAsync();
                foreach (var post in posts)
                {
                    post.Title = post.Title.Replace("1", "4").Replace("2", "8").Replace("3", "12");
                    post.Reads = 99;
                }
                await EFBatchOperation.For(db, db.BlogPosts).MergeAllAsync(posts,
                    c => c.ColumnsToIdentity(p => p.Title), 
                    c => c.ColumnsToUpdate(p => p.Reads));
            }

            using (var db = Context.Sql())
            {
                var posts = await db.BlogPosts.ToListAsync();
                Assert.IsFalse(posts.Where(p => p.Reads == 99).Select(p => p.Title).Except(new[] { "T4", "T8", "T12" }).Any());
                Assert.IsFalse(posts.Where(p => p.Reads == 0).Select(p => p.Title).Except(new[] { "T1", "T2", "T3" }).Any());
            }
        }

        private static async Task Setup()
        {
            using (var db = Context.Sql())
            {
                db.SetupDb();

                var list = new List<BlogPost>(){
                    BlogPost.Create("T1"),
                    BlogPost.Create("T2"),
                    BlogPost.Create("T3")
                };

                await EFBatchOperation.For(db, db.BlogPosts).InsertAllAsync(list);
            }
        }
    }
}
