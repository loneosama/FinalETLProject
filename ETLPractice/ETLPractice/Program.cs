using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.SqlClient;
using System.Data;
using ETLPractice;
using System.Security.Cryptography;
using System.Data.SqlTypes;
namespace ETLPractice
{
    class Program
    {
        static void Main(string[] args)
        {
            SqlConnection ETL_Conn = new SqlConnection("Server='127.0.0.1'; Database='ETLPractice'; User Id='sa'; password='Assasinking'");
            SqlConnection Stylo_Conn = new SqlConnection("Server='127.0.0.1'; Database='Stylo'; User Id='sa' ; password='Assasinking'");

            SqlCommand ETL_Cmd = new SqlCommand();
            SqlCommand Stylo_Cmd = new SqlCommand();
            ETL_Cmd.Connection = ETL_Conn;
            ETL_Conn.Open();
            Stylo_Cmd.Connection = Stylo_Conn;
            Stylo_Conn.Open();
            Stylo_Cmd.CommandText = "select * from stylodata";
            //var dr = ETL_Cmd.ExecuteReader();
            var da = new SqlDataAdapter(Stylo_Cmd);
            var dt = new DataTable();
            da.Fill(dt);

            int existing = 0;
            int inserted = 0;




            var Time = (from rows in dt.Rows.OfType<DataRow>()
                        select new
                        {
                            TransDate = rows["TransDate"],
                            Month = (int)rows["Month"],
                            Day = (int)rows["Day"],
                            d_season = rows["d_season"],
                            Year = (int)rows["Year"],
                        }).Distinct();


            // // bulk copy code

            ////var timedt = new DataTable("Time");
            ////timedt.Columns.Add("Month");
            ////timedt.Columns.Add("Day");
            ////timedt.Columns.Add("Year");
            ////timedt.Columns.Add("d_season");
            ////timedt.Columns.Add("Date");

            ////foreach (var time in Time)
            ////{
            ////    var row = timedt.NewRow();
            ////    row["Month"] = time.Month;
            ////    row["Day"] = time.Day;
            ////    row["d_season"] = time.d_season;
            ////    row["Year"] = time.Year;
            ////    row["Date"] = time.TransDate.ToString();
            ////    timedt.Rows.Add(row);
            ////}
            ////SqlBulkCopy bulkcopy = new SqlBulkCopy(connection: ETL_Conn);

            ////bulkcopy.DestinationTableName = "Time";

            ////bulkcopy.SqlRowsCopied += (s, e) => { Console.WriteLine(e.RowsCopied + " rows inserted: "); };
            ////bulkcopy.WriteToServer(timedt);


            //foreach (var time in Time)
            //{
            //    ETL_Cmd.CommandText = "SELECT Date from Time WHERE Date = '" + time.TransDate.ToString().Split()[0] + "'";
            //    var value = ETL_Cmd.ExecuteScalar();
            //    if (value == null)
            //    {
            //        var date = time.TransDate.ToString().Split();

            //        var month = "";

            //        if (time.Month == 1)
            //        {
            //            month = "January";
            //        }

            //        else if (time.Month == 2)
            //        {
            //            month = "February";
            //        }

            //        else if (time.Month == 3)
            //        {
            //            month = "March";
            //        }

            //        else if (time.Month == 4)
            //        {
            //            month = "April";
            //        }

            //        else if (time.Month == 5)
            //        {
            //            month = "May";
            //        }

            //        else if (time.Month == 6)
            //        {
            //            month = "June";
            //        }

            //        else if (time.Month == 7)
            //        {
            //            month = "July";
            //        }

            //        else if (time.Month == 8)
            //        {
            //            month = "August";
            //        }

            //        else if (time.Month == 9)
            //        {
            //            month = "September";
            //        }

            //        else if (time.Month == 10)
            //        {
            //            month = "October";
            //        }

            //        else if (time.Month == 11)
            //        {
            //            month = "November";
            //        }

            //        else if (time.Month == 12)
            //        {
            //            month = "Decemeber";
            //        }


            //        ETL_Cmd.CommandText = "INSERT INTO TIME(Date, d_season, Day, Month, Year) VALUES ('" + date[0] + "' , '" + time.d_season.ToString() + "' , " + time.Day + " , '" + month + "' , " + time.Year + ");";
            //        ETL_Cmd.ExecuteNonQuery();
            //        inserted++;
            //        if (inserted % 1000 == 0)
            //        {
            //            Console.WriteLine("" + inserted + " Time Records Inserted");
            //        }
            //    }

            //    else
            //    {
            //        existing++;
            //        if (existing % 1000 == 0)
            //        {
            //            Console.WriteLine("" + existing + " Time Records Found");
            //        }
            //        continue;
            //    }
            //}



            //existing = 0;
            //inserted = 0;

            //Console.WriteLine("Time Inserted");


            //var Location = (from rows in dt.Rows.OfType<DataRow>()
            //               select new
            //               {
            //                   Region = rows["Region"],
            //                   StoreID = rows["StoreID"],

            //               }).Distinct();

            //foreach (var Loc in Location)
            //{
            //    var hash = CalculateMD5Hash(Loc.Region.ToString() + Loc.StoreID.ToString());
            //    ETL_Cmd.CommandText = "SELECT Region_id from Location WHERE region_id = '" + hash + "'";
            //    var value = ETL_Cmd.ExecuteScalar();
            //    if (value == null)
            //    {
            //        ETL_Cmd.CommandText = "INSERT INTO Location(Region_id, Region, StoreID) VALUES ('" + hash + "' , '" + Loc.Region + "' , " + Loc.StoreID + "); ";
            //        ETL_Cmd.ExecuteNonQuery();
            //        inserted++;
            //        if (inserted % 1000 == 0)
            //        {
            //            Console.WriteLine("" + inserted + " Location Records Inserted");
            //        }
            //    }

            //    else
            //    {
            //        existing++;
            //        if (existing % 1000 == 0)
            //        {
            //            Console.WriteLine("" + existing + " Location Records Found");
            //        }
            //        continue;
            //    }
            //}



            //existing = 0;
            //inserted = 0;

            //Console.WriteLine("Location Inserted");





            //var Item = (from rows in dt.Rows.OfType<DataRow>()
            //           select new
            //           {
            //               ItemID = rows["ItemID"],
            //               Color = rows["Color"],
            //               Size = rows["Size"],
            //               Price = rows["Price"],
            //               Department = rows["Department"],
            //               Category = rows["Category"],
            //           }).Distinct();



            //foreach(var it in Item)
            //{
            //    var hash = CalculateMD5Hash(it.ItemID.ToString() + it.Color + it.Size + it.Price + it.Department + it.Category);
            //    ETL_Cmd.CommandText = "SELECT itemPK FROM Item where itemPK = '" + hash + "'";
            //    var value = ETL_Cmd.ExecuteScalar();
            //    if(value == null)
            //    {
            //        ETL_Cmd.CommandText = "INSERT INTO Item(itemPK, itemID, Color, Size, Price, Department, Category) Values ('" + hash + "', '" + it.ItemID.ToString() + "', '" + it.Color.ToString() + "', '" + it.Size + "', " + it.Price + " , '" + it.Department.ToString() + "' , '" + it.Category.ToString() + "');";
            //        ETL_Cmd.ExecuteNonQuery();
            //        inserted++;
            //        if (inserted % 1000 == 0)
            //        {
            //            Console.WriteLine("" + inserted + " Item Records Inserted");
            //        }
            //    }

            //    else
            //    {
            //        existing++;
            //        if (existing % 1000 == 0)
            //        {
            //            Console.WriteLine("" + existing + " Item Records Found");
            //        }
            //        continue;
            //    }
            //}



            //existing = 0;
            //inserted = 0;

            //Console.WriteLine("Item Inserted");


            //var Customer = (from rows in dt.Rows.OfType<DataRow>()
            //               select new
            //               {
            //                   CustomerAccount = rows["CustomerAccount"],
            //                   segments = rows["segments"],
            //                   CardStatus = rows["CardStatus"],
            //               }).Distinct();


            //foreach(var cus in Customer)
            //{
            //    var hash = CalculateMD5Hash(cus.CustomerAccount.ToString() + cus.segments.ToString() + cus.CardStatus.ToString());
            //    ETL_Cmd.CommandText = "SELECT Customer_id FROM Customer where Customer_id = '"+hash+"'";
            //    var value = ETL_Cmd.ExecuteScalar();
            //    if (value == null)
            //    {
            //        ETL_Cmd.CommandText = "INSERT INTO Customer(Customer_id, Cust_Account, segment, Card_Status) Values ('" + hash + "', '" + cus.CustomerAccount.ToString() + "', '" + cus.segments.ToString() + "', '" + cus.CardStatus.ToString() + "');";
            //        ETL_Cmd.ExecuteNonQuery();
            //        inserted++;
            //        if (inserted % 1000 == 0)
            //        {
            //            Console.WriteLine("" + inserted + " Customer Records Inserted");
            //        }
            //    }

            //    else
            //    {
            //        existing++;
            //        if (existing % 1000 == 0)
            //        {
            //            Console.WriteLine("" + existing + " Customer Records Found");
            //        }
            //        continue;
            //    }
            //}



            //existing = 0;
            //inserted = 0;

            //Console.WriteLine("Customer Inserted");


            // Populate Fact Table

            var Sales = (from rows in dt.Rows.OfType<DataRow>()
                         select new
                         {
                             TransactionID = rows["TransactionID"],
                             SoldQty = rows["SoldQty"],
                             TransAmt = rows["TransAmt"],
                             TransDate = rows["TransDate"],
                             Month = (int)rows["Month"],
                             Day = (int)rows["Day"],
                             d_season = rows["d_season"],
                             Year = (int)rows["Year"],
                             CustomerAccount = rows["CustomerAccount"],
                             segments = rows["segments"],
                             CardStatus = rows["CardStatus"],
                             ItemID = rows["ItemID"],
                             Color = rows["Color"],
                             Size = rows["Size"],
                             Price = rows["Price"],
                             Department = rows["Department"],
                             Category = rows["Category"],
                             Region = rows["Region"],
                             StoreID = rows["StoreID"],

                         }).Distinct();



            var Saledt = new DataTable("Sales");
            Saledt.Columns.Add("Sale_ID");
            Saledt.Columns.Add("Trans_ID");
            Saledt.Columns.Add("Trans_Amt");
            Saledt.Columns.Add("Sold_Qty");
            Saledt.Columns.Add("Customer_id");
            Saledt.Columns.Add("Region_id");
            Saledt.Columns.Add("Date");
            Saledt.Columns.Add("ItemID");


            foreach (var sale in Sales)
            {

                var row = Saledt.NewRow();
                row["Sale_id"] = CalculateMD5Hash(sale.TransactionID.ToString() + sale.TransAmt + sale.SoldQty);
                row["Trans_ID"] = sale.TransactionID.ToString();
                row["Trans_Amt"] = sale.TransAmt.ToString();
                row["Sold_Qty"] = (int)sale.SoldQty;
                row["Customer_id"] = CalculateMD5Hash(sale.CustomerAccount.ToString() + sale.segments.ToString() + sale.CardStatus.ToString());
                row["Region_id"] = CalculateMD5Hash(sale.Region.ToString() + sale.StoreID.ToString());
                row["ItemID"] = CalculateMD5Hash(sale.ItemID.ToString() + sale.Color + sale.Size + sale.Price + sale.Department + sale.Category);
                row["Date"] = sale.TransDate;

                Saledt.Rows.Add(row);
            }
            SqlBulkCopy bulkcopy = new SqlBulkCopy(connection: ETL_Conn);

            bulkcopy.ColumnMappings.Add("Sale_id", "Sale_ID");
            bulkcopy.ColumnMappings.Add("Trans_ID", "Trans_ID");
            bulkcopy.ColumnMappings.Add("Trans_Amt", "Trans_Amt");
            bulkcopy.ColumnMappings.Add("Sold_Qty", "Sold_Qty");
            bulkcopy.ColumnMappings.Add("Customer_id", "Customer_id");
            bulkcopy.ColumnMappings.Add("Region_id", "Region_id");
            bulkcopy.ColumnMappings.Add("Date", "Date");
            bulkcopy.ColumnMappings.Add("ItemID", "ItemID");

            bulkcopy.DestinationTableName = "Sales";

            bulkcopy.SqlRowsCopied += (s, e) => { Console.WriteLine(e.RowsCopied + " rows inserted: "); };
            bulkcopy.NotifyAfter = 1000;
            bulkcopy.WriteToServer(Saledt);





        }

        private static void Bulkcopy_SqlRowsCopied(object sender, SqlRowsCopiedEventArgs e)
        {
            throw new NotImplementedException();
        }

        //public static void Insert(IEnumerable<T> items)
        //{

        //}

        public static string CalculateMD5Hash(string input)
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
}
