using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.IO;
using System.Linq;

namespace DatabaseComparer
{
    class Program
    {
        public static bool Error { get; set; }

        static void Main(string[] args)
        {
            Console.Title = "DataBase Comparer";

            Error = false;

            string primaryDatabase,
                secondaryDatabase,
                primaryServer,
                secondaryServer,
                primaryConnectionString,
                secondaryConnectionString,
                query;

            Console.Write("Primary server: ");

            primaryServer = Console.ReadLine();

            Console.Write("Primary database: ");

            primaryDatabase = Console.ReadLine();

            Console.Write("Secondary server: ");

            secondaryServer = Console.ReadLine();

            Console.Write("Secondary database: ");

            secondaryDatabase = Console.ReadLine();

            primaryConnectionString = $@"Server={primaryServer};Database={primaryDatabase};Integrated Security = true;";
            secondaryConnectionString = $@"Server={secondaryServer};Database={secondaryDatabase};Integrated Security = true;";

            query = @"
    SELECT T.name AS Table_Name ,
       C.name AS Column_Name ,
       P.name AS Data_Type ,
       C.max_length AS Size ,
	   P.is_nullable AS Is_Nullable ,
       CAST(P.precision AS VARCHAR) + '/' + CAST(P.scale AS VARCHAR) AS Precision_Scale
FROM sys.objects AS T
     JOIN sys.columns AS C ON T.object_id = C.object_id
       JOIN sys.types AS P ON C.system_type_id = P.system_type_id
WHERE T.type_desc = 'USER_TABLE'; ";

            

            try
            {
                List<TableObject> primaryDatabaseSchema = GenerarListado(primaryConnectionString, query);
                List<TableObject> secondaryDatabaseSchema = GenerarListado(secondaryConnectionString, query);

                StreamWriter sw = new StreamWriter(Environment.GetFolderPath(Environment.SpecialFolder.DesktopDirectory) + $@"\[{primaryDatabase}] vs  [{secondaryDatabase}] diferences.txt", false);
                sw.WriteLine($"[{primaryDatabase}] vs [{secondaryDatabase}]");
                sw.WriteLine();

                Console.ForegroundColor = ConsoleColor.Magenta;

                Console.WriteLine($"[{primaryDatabase}] vs [{secondaryDatabase}]");
                Console.WriteLine();

                Analizar(primaryDatabase, primaryDatabaseSchema, secondaryDatabaseSchema, sw);

                Console.ForegroundColor = ConsoleColor.Yellow;

                #region Divider
                Console.WriteLine();
                Console.WriteLine();
                Console.WriteLine();

                Console.WriteLine(" ============================================================================================ ");

                Console.WriteLine();
                Console.WriteLine();
                Console.WriteLine();

                sw.WriteLine();
                sw.WriteLine();
                sw.WriteLine();

                sw.WriteLine(" ============================================================================================ ");

                sw.WriteLine();
                sw.WriteLine();
                sw.WriteLine();
                #endregion

                Analizar(secondaryDatabase, secondaryDatabaseSchema, primaryDatabaseSchema, sw);

                sw.Close();
            }
            catch (Exception ex)
            {
                StreamWriter sw = new StreamWriter(Environment.GetFolderPath(Environment.SpecialFolder.DesktopDirectory) + @"\ErrorLog.txt");
                sw.Write(ex);
                sw.Close();

                Console.WriteLine(ex);
            }

            Console.WriteLine();

            Console.WriteLine("Ready!");

            Console.ReadKey();
        }

        public static List<TableObject> GenerarListado(string connectionString, string query)
        {
            List<TableObject> tableList = new List<TableObject>();
            List<ColumnObject> columnList = new List<ColumnObject>();

            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                SqlCommand command = new SqlCommand(query, connection);
                connection.Open();
                SqlDataReader reader = command.ExecuteReader();
                try
                {
                    while (reader.Read())
                    {
                        if (!tableList.Any(x => x.Name.Equals(reader["Table_Name"])))
                        {
                            TableObject table = new TableObject
                            {
                                Name = reader["Table_Name"].ToString(),
                                Columns = new List<ColumnObject>()
                            };

                            if(table.Name != "sysdiagrams")
                                tableList.Add(table);
                        }

                        ColumnObject column = new ColumnObject
                        {
                            TableName = reader["Table_Name"].ToString(),
                            Name = reader["Column_Name"].ToString(),
                            ColumnType = reader["Data_Type"].ToString(),
                            Size = reader["Size"].ToString(),
                            Nullable = reader["Is_Nullable"].ToString() == "True" ? "NULL" : "NOT NULL"
                        };

                        if(column.ColumnType != "sysname" && column.TableName != "sysdiagrams")
                            columnList.Add(column);
                    }

                    foreach (ColumnObject column in columnList)
                    {
                        foreach (TableObject table in tableList)
                        {
                            if (column.TableName.Equals(table.Name))
                                table.Columns.Add(column);
                        }
                    }
                }
                finally
                {
                    // Always call Close when done reading.
                    reader.Close();
                }

                return tableList;
            }
        }

        public static void Analizar(string masterDatabase, List<TableObject> masterSchema, List<TableObject> slaveSchema, StreamWriter sw)
        {
            Console.WriteLine($" === [{masterDatabase}] analysis === ");
            sw.WriteLine($" === [{masterDatabase}] analysis === ");

            #region Tablas, Columnas y tipos
            //foreach (TableObject table in slaveSchema)
            //{
            //    Console.WriteLine($"Analizando [{table.Name}]");

            //    if (!masterSchema.Any(x => x.Name.ToLower().Equals(table.Name.ToLower())))
            //    {
            //        Error = true;

            //        sw.WriteLine($"• Tabla faltante: [{table.Name}] en [{masterDatabase}]");
            //        Console.WriteLine($"Tabla faltante: [{table.Name}] en [{masterDatabase}]");
            //    }
            //    else
            //    {
            //        foreach (ColumnObject column in table.Columns)
            //        {
            //            if (!masterSchema.Any(x => x.Columns.Any(y => y.Name.ToLower().Equals(column.Name.ToLower()))))
            //            {
            //                Error = true;

            //                sw.WriteLine($"• Columna faltante: [{column.Name}] ({column.ColumnType}({column.Size}) {column.Nullable}) en la Tabla: [{table.Name}] en [{masterDatabase}]");
            //                Console.WriteLine($"Columna faltante: [{column.Name}] ({column.ColumnType}({column.Size}) {column.Nullable}) en la Tabla: [{table.Name}] en [{masterDatabase}]");
            //            }

            //            if (masterSchema.Any(x => x.Columns.Any(y => y.TableName.Equals(column.TableName)
            //            && y.Name.Equals(column.Name)
            //            && !y.ColumnType.Equals(column.ColumnType))))
            //            {
            //                Error = true;

            //                sw.WriteLine($"• Tipo de dato diferente: [{column.Name}] ({column.ColumnType}({column.Size}) {column.Nullable}) en la Tabla: [{table.Name}] en [{masterDatabase}]");
            //                Console.WriteLine($"Tipo de dato diferente: [{column.Name}] ({column.ColumnType}({column.Size}) {column.Nullable}) en la Tabla: [{table.Name}] en [{masterDatabase}]");
            //            }

            //            if (masterSchema.Any(x => x.Columns.Any(y => y.TableName.Equals(column.TableName)
            //            && y.Name.Equals(column.Name)
            //            && !y.Size.Equals(column.Size))))
            //            {
            //                Error = true;

            //                sw.WriteLine($"• Tamaño de dato diferente: [{column.Name}] ({column.ColumnType}({column.Size}) {column.Nullable}) en la Tabla: [{table.Name}] en [{masterDatabase}]");
            //                Console.WriteLine($"Tamaño de dato diferente: [{column.Name}] ({column.ColumnType}({column.Size}) {column.Nullable}) en la Tabla: [{table.Name}] en [{masterDatabase}]");
            //            }
            //        }
            //    }
            //}
            #endregion

            if (!Error)
                sw.WriteLine("Sin diferencias...");
        }

        public class TableObject
        {
            public string Name { get; set; }
            public List<ColumnObject> Columns { get; set; }
        }

        public class ColumnObject
        {
            public string TableName { get; set; }
            public string Name { get; set; }
            public string ColumnType { get; set; }
            public string Size { get; set; }
            public string Nullable { get; set; }
            public List<Constraint> Constraint { get; set; }
            public List<Index> Indexes { get; set; }
        }

        public class Constraint
        {
            public string Name { get; set; }
            public string Is_Primary { get; set; }
            public string References { get; set; }            
        }

        public class Index
        {
            public string Name { get; set; }
            public string Is_Unique { get; set; }
            public string Clustered { get; set; }
        }
    }
}
