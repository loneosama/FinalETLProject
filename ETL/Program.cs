using System;
using System.Collections;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;
using System.Reflection;

namespace ETL
{
    class Program
    {
        static void Main(string[] args)
        {
            Stopwatch stopwatch = new Stopwatch();
            stopwatch.Start();

            //ETL Location Dimension
            Location l = new Location();
            l.GetLocations();
            l.Insert();

            //ETL Customer Dimension
            Customer c = new Customer();
            c.GetCustomers();
            c.Insert();

            //ETL Item Dimension
            Item i = new Item();
            i.GetItems();
            i.Insert();

            //ETL Time Dimension
            Date d = new Date();
            d.GetDates();
            d.Insert();

            //ETL Sales FactTable
            Sales s = new Sales();
            s.GetSales();
            s.BulkInsert();

            stopwatch.Stop();
            Console.WriteLine("Time elapsed: {0}", stopwatch.Elapsed);
        }
        public static string CalculateMd5Hash(string input)
        {
            // step 1, calculate MD5 hash from input
            MD5 md5 = System.Security.Cryptography.MD5.Create();
            byte[] inputBytes = System.Text.Encoding.ASCII.GetBytes(input);
            byte[] hash = md5.ComputeHash(inputBytes);

            // step 2, convert byte array to hex string
            StringBuilder sb = new StringBuilder();

            for (int i = 0; i < hash.Length; i++)
            {
                sb.Append(hash[i].ToString("x2"));
            }
            return sb.ToString();
        }
    }

    public class Location
    {
        public string RegionId { get; set; }
        public string Region { get; set; }
        public int StoreId { get; set; }

        public List<Location> LocationList = new List<Location>();

        public void Load(SqlDataReader reader)
        {
            Region = reader["Region"].ToString();
            StoreId = Int32.Parse(reader["StoreID"].ToString());
            RegionId = Program.CalculateMd5Hash(Region + StoreId);
        }
        public List<Location> GetLocations()
        {
            Location e;
            using (SqlConnection conn = new SqlConnection("Data Source=127.0.0.1;Initial Catalog=ETL;Integrated Security=True"))
            {
                conn.Open();
                using (SqlCommand cmd = conn.CreateCommand())
                {
                    cmd.CommandText = "SELECT DISTINCT Region, StoreID FROM ETL.ETL";
                    SqlDataReader reader = cmd.ExecuteReader();
                    while (reader.Read())
                    {
                        e = new Location();
                        e.Load(reader);
                        LocationList.Add(e);
                    }
                }
                conn.Close();
            }
            return LocationList;
        }

        public void Insert()
        {
            if (LocationList.Count != 0)
            {
                using (
                    SqlConnection conn =
                        new SqlConnection("Data Source=127.0.0.1;Initial Catalog=ETLed;Integrated Security=True"))
                {
                    conn.Open();
                    using (SqlCommand cmd = conn.CreateCommand())
                    {
                        foreach (var location in LocationList)
                        {
                            cmd.CommandText = "INSERT INTO ETLed.Location(RegionID, Region, StoreID) VALUES ('{0}' , '{1}' , '{2}')";
                            cmd.CommandText = string.Format(cmd.CommandText, location.RegionId, location.Region, (int)location.StoreId);
                            cmd.ExecuteNonQuery();
                            
                        }
                    }
                    conn.Close();
                }
            }
        }
    }

    public class Customer
    {
        public string CustomerId { get; set; }
        public string Segment { get; set; }
        public string CardStatus { get; set; }
        public string CustomerAccount { get; set; }

        public List<Customer> CustomerList = new List<Customer>();

        public void Load(SqlDataReader reader)
        {
            Segment = reader["segments"].ToString();
            CardStatus = reader["CardStatus"].ToString();
            CustomerAccount = reader["CustomerAccount"].ToString();
            CustomerId = Program.CalculateMd5Hash(Segment + CardStatus + CustomerAccount);

        }
        public List<Customer> GetCustomers()
        {
            Customer e;
            using (SqlConnection conn = new SqlConnection("Data Source=127.0.0.1;Initial Catalog=ETL;Integrated Security=True"))
            {
                conn.Open();
                using (SqlCommand cmd = conn.CreateCommand())
                {
                    cmd.CommandText = "SELECT DISTINCT segments, CardStatus, CustomerAccount FROM ETL.ETL";
                    SqlDataReader reader = cmd.ExecuteReader();
                    while (reader.Read())
                    {
                        e = new Customer();
                        e.Load(reader);
                        CustomerList.Add(e);
                    }
                }
                conn.Close();
            }
            return CustomerList;
        }

        public void Insert()
        {
            if (CustomerList.Count != 0)
            {
                using (
                    SqlConnection conn =
                        new SqlConnection("Data Source=127.0.0.1;Initial Catalog=ETLed;Integrated Security=True"))
                {
                    conn.Open();
                    using (SqlCommand cmd = conn.CreateCommand())
                    {
                        foreach (var customer in CustomerList)
                        {
                            cmd.CommandText = "INSERT INTO ETLed.Customer(CustomerID, Segment, CardStatus, CustomerAccount) VALUES ('{0}' , '{1}' , '{2}', '{3}')";
                            cmd.CommandText = string.Format(cmd.CommandText, customer.CustomerId, customer.Segment, customer.CardStatus, customer.CustomerAccount);
                            cmd.ExecuteNonQuery();
                        }
                    }
                    conn.Close();
                }
            }
        }
    }

    public class Item
    {
        public string ItemRowId { get; set; }
        public string ItemId { get; set; }
        public int Size { get; set; }
        public int Price { get; set; }
        public string Department { get; set; }
        public string Category { get; set; }
        public string Color { get; set; }

        public List<Item> ItemList = new List<Item>();

        public void Load(SqlDataReader reader)
        {
            ItemId = reader["ItemID"].ToString();
            Size = Int32.Parse(reader["Size"].ToString());
            Price = Int32.Parse(reader["Price"].ToString());
            Department = reader["Department"].ToString();
            Category = reader["Category"].ToString();
            Color = reader["Color"].ToString();
            ItemRowId = Program.CalculateMd5Hash(ItemId + Size + Price + Department + Category + Color);
        }
        public List<Item> GetItems()
        {
            Item e;
            using (SqlConnection conn = new SqlConnection("Data Source=127.0.0.1;Initial Catalog=ETL;Integrated Security=True"))
            {
                conn.Open();
                using (SqlCommand cmd = conn.CreateCommand())
                {
                    cmd.CommandText = "SELECT DISTINCT ItemID, Size, Price, Department, Category, Color FROM ETL.ETL";
                    SqlDataReader reader = cmd.ExecuteReader();
                    while (reader.Read())
                    {
                        e = new Item();
                        e.Load(reader);
                        ItemList.Add(e);
                    }
                }
                conn.Close();
            }
            return ItemList;
        }

        public void Insert()
        {
            if (ItemList.Count != 0)
            {
                using (
                    SqlConnection conn =
                        new SqlConnection("Data Source=127.0.0.1;Initial Catalog=ETLed;Integrated Security=True"))
                {
                    conn.Open();
                    using (SqlCommand cmd = conn.CreateCommand())
                    {
                        foreach (var item in ItemList)
                        {
                            cmd.CommandText = "INSERT INTO ETLed.Item(ItemRowID, ItemID, Size, Price, Department, Category, Color) VALUES ('{0}' , '{1}' , '{2}', '{3}', '{4}', '{5}', '{6}')";
                            cmd.CommandText = string.Format(cmd.CommandText, item.ItemRowId, item.ItemId, item.Size, item.Price, item.Department, item.Category, item.Color);
                            cmd.ExecuteNonQuery();
                        }
                    }
                    conn.Close();
                }
            }
        }
    }
        
    public class Date
    {
        public string DateId { get; set; }
        public string Season { get; set; }
        public int Day { get; set; }
        public int Month { get; set; }
        public int Year { get; set; }

        public List<Date> DateList = new List<Date>();

        public void Load(SqlDataReader reader)  
        {
            DateId = reader["TransDate"].ToString();
            Season = reader["d_season"].ToString();
            Day = Int32.Parse(reader["Day"].ToString());
            Month = Int32.Parse(reader["Month"].ToString());
            Year = Int32.Parse(reader["Year"].ToString());
        }
        public List<Date> GetDates()
        {
            Date e;
            using (SqlConnection conn = new SqlConnection("Data Source=127.0.0.1;Initial Catalog=ETL;Integrated Security=True"))
            {
                conn.Open();
                using (SqlCommand cmd = conn.CreateCommand())
                {
                    cmd.CommandText = "SELECT DISTINCT TransDate, d_season, Month, Day, Year FROM ETL.ETL";
                    SqlDataReader reader = cmd.ExecuteReader();
                    while (reader.Read())
                    {
                        e = new Date();
                        e.Load(reader);
                        DateList.Add(e);
                    }
                }
                conn.Close();
            }
            return DateList;
        }

        public void Insert()
        {
            if (DateList.Count != 0)
            {
                using (
                    SqlConnection conn =
                        new SqlConnection("Data Source=127.0.0.1;Initial Catalog=ETLed;Integrated Security=True"))
                {
                    conn.Open();
                    using (SqlCommand cmd = conn.CreateCommand())
                    {
                        foreach (var date in DateList)
                        {
                            cmd.CommandText = "INSERT INTO ETLed.Time(Date, Season, Day, Month, Year) VALUES ('{0}' , '{1}' , '{2}', '{3}', '{4}')";
                            cmd.CommandText = string.Format(cmd.CommandText, date.DateId, date.Season, date.Day, date.Month, date.Year);
                            cmd.ExecuteNonQuery();
                        }
                    }
                    conn.Close();
                }
            }
        }
    }

    public class Sales
    {
        public int TransactionId { get; set; }
        public int TransAmt { get; set; }
        public int SoldQty { get; set; }
        public Date Date { get; set; }
        //Location Table Fields
        public string RegionId { get; set; }
        public string Region { get; set; }
        public int StoreId { get; set; }
        //Customer Table Fields
        public string CustomerId { get; set; }
        public string Segment { get; set; }
        public string CardStatus { get; set; }
        public string CustomerAccount { get; set; }
        //Item Table Fields
        public string ItemRowId { get; set; }
        public string ItemId { get; set; }
        public int Size { get; set; }
        public int Price { get; set; }
        public string Department { get; set; }
        public string Category { get; set; }
        public string Color { get; set; }
        //Time Table Fields
        public string DateId { get; set; }
        public string Season { get; set; }
        public int Day { get; set; }
        public int Month { get; set; }
        public int Year { get; set; }


        public List<Sales> SalesList = new List<Sales>();

        public void Load(SqlDataReader reader)
        {
            //Sales Table Fields
            TransactionId = Int32.Parse(reader["TransactionID"].ToString());
            TransAmt = Int32.Parse(reader["TransAmt"].ToString());
            SoldQty = Int32.Parse(reader["SoldQty"].ToString());
            //Location Table Fields
            Region = reader["Region"].ToString();
            StoreId = Int32.Parse(reader["StoreID"].ToString());
            RegionId = Program.CalculateMd5Hash(Region + StoreId);
            //Customer Table Fields
            Segment = reader["segments"].ToString();
            CardStatus = reader["CardStatus"].ToString();
            CustomerAccount = reader["CustomerAccount"].ToString();
            CustomerId = Program.CalculateMd5Hash(Segment + CardStatus + CustomerAccount);
            //Item Table Fields
            ItemId = reader["ItemID"].ToString();
            Size = Int32.Parse(reader["Size"].ToString());
            Price = Int32.Parse(reader["Price"].ToString());
            Department = reader["Department"].ToString();
            Category = reader["Category"].ToString();
            Color = reader["Color"].ToString();
            ItemRowId = Program.CalculateMd5Hash(ItemId + Size + Price + Department + Category + Color);
            //Date Table Fields
            DateId = reader["TransDate"].ToString();
            Season = reader["d_season"].ToString();
            Day = Int32.Parse(reader["Day"].ToString());
            Month = Int32.Parse(reader["Month"].ToString());
            Year = Int32.Parse(reader["Year"].ToString());
        }
        public List<Sales> GetSales()
        {
            Sales e;
            using (SqlConnection conn = new SqlConnection("Data Source=127.0.0.1;Initial Catalog=ETL;Integrated Security=True"))
            {
                conn.Open();
                using (SqlCommand cmd = conn.CreateCommand())
                {
                    cmd.CommandText = "SELECT CustomerAccount, TransactionID, TransDate, StoreID, ItemID, Color, Size, SoldQty, TransAmt, d_season, Region, Price, sole, Department, Category, segments, CardStatus, Month, Day, Year FROM ETL.ETL";
                    SqlDataReader reader = cmd.ExecuteReader();
                    while (reader.Read())
                    {
                        e = new Sales();
                        e.Load(reader);
                        SalesList.Add(e);
                    }
                }
                conn.Close();
            }
            return SalesList;
        }

        public void Insert()
        {
            if (SalesList.Count != 0)
            {
                using (
                    SqlConnection conn =
                        new SqlConnection("Data Source=127.0.0.1;Initial Catalog=ETLed;Integrated Security=True"))
                {
                    conn.Open();
                    using (SqlCommand cmd = conn.CreateCommand())
                    {
                        foreach (var sales in SalesList)
                        {
                            cmd.CommandText = "INSERT INTO ETLed.Sales(TransactionID, TransAmt, SoldQty, CustomerID, RegionID, Date, ItemRowID) VALUES ('{0}' , '{1}' , '{2}', '{3}', '{4}', '{5}', '{6}')";
                            cmd.CommandText = string.Format(cmd.CommandText, sales.TransactionId, sales.TransAmt, sales.SoldQty, sales.CustomerId, sales.RegionId, sales.DateId, sales.ItemRowId);
                            cmd.ExecuteNonQuery();
                        }
                    }
                    conn.Close();
                }
            }
        }

        public void BulkInsert()
        {
            var dataTable = new System.Data.DataTable();
            dataTable.Columns.Add(new System.Data.DataColumn("TransactionID"));
            dataTable.Columns.Add(new System.Data.DataColumn("TransAmt"));
            dataTable.Columns.Add(new System.Data.DataColumn("SoldQty"));
            dataTable.Columns.Add(new System.Data.DataColumn("CustomerID"));
            dataTable.Columns.Add(new System.Data.DataColumn("RegionID"));
            dataTable.Columns.Add(new System.Data.DataColumn("Date"));
            dataTable.Columns.Add(new System.Data.DataColumn("ItemRowID"));
            //Console.WriteLine(dataTable.Columns.Count);
            foreach (var sale in SalesList)
            {
                System.Data.DataRow row = dataTable.NewRow();
                row["TransactionID"] = sale.TransactionId;
                row["TransAmt"] = sale.TransAmt;
                row["SoldQty"] = sale.SoldQty;
                row["CustomerID"] = sale.CustomerId;
                row["RegionID"] = sale.RegionId;
                row["Date"] = sale.DateId;
                row["ItemRowID"] = sale.ItemRowId;
                dataTable.Rows.Add(row);
            }
            //Console.WriteLine(dataTable.Rows.Count);
            SqlConnection connection = new SqlConnection("Data Source=127.0.0.1;Initial Catalog=ETLed;Integrated Security=True");
            connection.Open();
            SqlBulkCopy bulkcopy = new SqlBulkCopy(connection);
            SqlBulkCopyColumnMapping mapping = new SqlBulkCopyColumnMapping();
            bulkcopy.ColumnMappings.Add(new SqlBulkCopyColumnMapping("TransactionID", "TransactionID"));
            bulkcopy.ColumnMappings.Add(new SqlBulkCopyColumnMapping("TransAmt", "TransAmt"));
            bulkcopy.ColumnMappings.Add(new SqlBulkCopyColumnMapping("SoldQty", "SoldQty"));
            bulkcopy.ColumnMappings.Add(new SqlBulkCopyColumnMapping("CustomerID", "CustomerID"));
            bulkcopy.ColumnMappings.Add(new SqlBulkCopyColumnMapping("RegionID", "RegionID"));
            bulkcopy.ColumnMappings.Add(new SqlBulkCopyColumnMapping("Date", "Date"));
            bulkcopy.ColumnMappings.Add(new SqlBulkCopyColumnMapping("ItemRowID", "ItemRowID"));
            bulkcopy.DestinationTableName = "ETLed.Sales";
            bulkcopy.WriteToServer(dataTable);
            //By default BulkCopy Ignores all triggers and constraints so use this inorder to cater triggers and custom constraints
            //SqlBulkCopy bulkcopy = new SqlBulkCopy(connection: connection, externalTransaction: null, copyOptions: SqlBulkCopyOptions.FireTriggers | SqlBulkCopyOptions.CheckConstraints | SqlBulkCopyOptions);
            connection.Close();


        }
    }

}
