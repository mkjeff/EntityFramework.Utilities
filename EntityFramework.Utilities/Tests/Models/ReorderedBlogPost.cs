using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Text;

namespace Tests.FakeDomain.Models
{
    public class ReorderedBlogPost
    {
        [Column("BlogId")]
        public int ID { get; set; }
        public string ShortTitle { get; set; }
        public DateTime Created { get; set; }
        public string Title { get; set; } //<--- Reversed order of this and created for Batch Insert testing
        public int Reads { get; set; }
        public AuthorInfo Author { get; set; }

    }
}
