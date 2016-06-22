﻿using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Text;

namespace Tests.FakeDomain.Models
{
    public class RenamedAndReorderedBlogPost
    {
        [Column("BlogId")]
        public int ID { get; set; }
        public DateTime Created { get; set; }
        public string Title { get; set; }
        public int Reads { get; set; }

        public static string CreateTableSql()
        {
            return @"CREATE TABLE [dbo].[RenamedAndReorderedBlogPosts](
	[ID] [int] IDENTITY(1,1) NOT NULL,
	[Title] [nvarchar](max) NULL,
    [Created2] [datetime] NOT NULL,
	[Reads2] [int] NOT NULL,
 CONSTRAINT [PK_BlogPosts] PRIMARY KEY CLUSTERED 
(
	[ID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]";
        }

        public static RenamedAndReorderedBlogPost Create(string title)
        {

            return new RenamedAndReorderedBlogPost
            {
                Title = title,
                Created = DateTime.Now
            };
        }

    }

}
