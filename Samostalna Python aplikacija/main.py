from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import mean, min, max, stddev, variance, col
from pyspark.sql.functions import concat, date_format, date_trunc, lit, to_date, to_timestamp, to_utc_timestamp
from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType, Row
import sys

def Inicijalizacija():
    # conf = SparkConf()
    # conf.setMaster("spark://spark-master:7077")
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("Projekat") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    path = "2014-10/2014-10-"
    n = 3

    rdd = spark.sparkContext.textFile(path + "01.txt")
    for i in range(2, n + 1):
        if i < 10:
            rdd = rdd.union(spark.sparkContext.textFile(path + "0" + str(i) + ".txt"))
        else:
            rdd = rdd.union(spark.sparkContext.textFile(path + str(i) + ".txt"))

    parts = rdd.map(lambda l: l.split(","))
    podaci = parts.map(
        lambda p: Row(date=p[0], time=p[1], busID=p[2], busLine=p[3], latitude=p[4], longitude=p[5], speed=p[6]))
    bus_df = spark.createDataFrame(podaci)

    df = bus_df.withColumn("datetime", concat(col("date"), lit(" "), col("time")))
    df = df.withColumn("datetime", to_timestamp("datetime", "MM-dd-yyyy HH:mm:ss"))
    df = df.drop("date")
    df = df.drop("time")

    df = df.withColumn("latitude", df["latitude"].cast(DoubleType()))
    df = df.withColumn("longitude", df["longitude"].cast(DoubleType()))
    df = df.withColumn("speed", df["speed"].cast(DoubleType()))

    #brisanje pogresnih vrednosti (outliera)
    df = df.filter(df.speed <= 120)
    return df

def InicijalizacijaSamoVremena(date1, date2):
    if len(sys.argv) == 3:

        start_time_str = sys.argv[1]
        end_time_str = sys.argv[2]

        date1Str = str(date1)
        date2Str = str(date2)

        date1Str = date1Str[:10]
        date2Str = date2Str[:10]

        date1Str = date1Str + " " + start_time_str
        date2Str = date2Str + " " +end_time_str

        return datetime.strptime(date1Str, "%Y-%m-%d %H:%M:%S"), datetime.strptime(date2Str, "%Y-%m-%d %H:%M:%S")

def InicijalizacijaSamoDatuma():
    if len(sys.argv) == 3:
        arg1 = sys.argv[1]
        arg2 = sys.argv[2]
        str1 = '10-'
        str2 = '-2014 '
        str = "00:00:00"
        datum1Str = f"{str1}{arg1.zfill(2)}{str2}{str}"
        datum2Str = f"{str1}{arg2.zfill(2)}{str2}{str}"

        date1 = datetime.strptime(datum1Str, "%m-%d-%Y %H:%M:%S")
        date2 = datetime.strptime(datum2Str, "%m-%d-%Y %H:%M:%S")

        return date1, date2

def InicijalizacijaDatumaIVremena():
    if len(sys.argv) == 5:
        arg1 = sys.argv[1]
        arg2 = sys.argv[3]
        arg3 = sys.argv[2]
        arg4 = sys.argv[4]
        # Konkatenacija
        str1 = "10-"
        str2 = "-2014 "

        datum1Str = f"{str1}{arg1.zfill(2)}{str2}{arg2}"
        datum2Str = f"{str1}{arg3.zfill(2)}{str2}{arg4}"
    else:
        arg1 = sys.argv[5]
        arg2 = sys.argv[7]
        arg3 = sys.argv[6]
        arg4 = sys.argv[8]
        # Konkatenacija
        str1 = "10-"
        str2 = "-2014 "

        datum1Str = f"{str1}{arg1.zfill(2)}{str2}{arg2}"
        datum2Str = f"{str1}{arg3.zfill(2)}{str2}{arg4}"

    # castovanje
    date1 = datetime.strptime(datum1Str, "%m-%d-%Y %H:%M:%S")
    date2 = datetime.strptime(datum2Str, "%m-%d-%Y %H:%M:%S")

    return date1, date2

def isfloat(num):
    try:
        float(num)
        return True
    except ValueError:
        return False

def VratiProsecneBrzinePoTrasi(df,long1=None, long2=None, lat1=None, lat2=None, date1=None, date2=None):
    if (long1 != None) & (long2 != None) & (lat1 != None) & (lat2 != None) & (date1 == None) & (date2 == None):
        df_ret = df.where(
            (df.longitude < long1) &
            (df.longitude > long2) &
            (df.latitude < lat1) &
            (df.latitude > lat2)
        ).groupBy("busLine").agg(mean("speed"))

    elif (long1 != None) & (long2 != None) & (lat1 != None) & (lat2 != None) & (date1 != None) & (date2 != None):
        df_ret = df.where(
            (df.datetime > date1) &
            (df.datetime < date2) &
            (df.longitude < long1) &
            (df.longitude > long2) &
            (df.latitude < lat1) &
            (df.latitude > lat2)
        ).groupBy("busLine").agg(mean("speed"))

    elif (long1 == None) & (long2 == None) & (lat1 == None) & (lat2 == None) & (date1 != None) & (date2 != None):
        df_ret = df.where(
            (df.datetime > date1) &
            (df.datetime < date2)
        ).groupBy("busLine").agg(mean("speed"))

    return df_ret

def VratiStatistickeVrednosti(df,long1=None, long2=None, lat1=None, lat2=None, date1=None, date2=None):
    if (long1 != None) & (long2 != None) & (lat1 != None) & (lat2 != None) & (date1 == None) & (date2 == None):
        df_ret = df.where(
            (df.longitude < long1) &
            (df.longitude > long2) &
            (df.latitude < lat1) &
            (df.latitude > lat2)
        ).agg(
            mean(df.speed).alias("mean_speed"),
            variance(df.speed).alias("variance_speed"),
            stddev(df.speed).alias("stddev_speed"),
            min(df.speed).alias("min_speed"),
            max(df.speed).alias("max_speed")
        )
    elif (long1 != None) & (long2 != None) & (lat1 != None) & (lat2 != None) & (date1 != None) & (date2 != None):
        df_ret = df.where(
            (df.datetime > date1) &
            (df.datetime < date2) &
            (df.longitude < long1) &
            (df.longitude > long2) &
            (df.latitude < lat1) &
            (df.latitude > lat2)
        ).agg(
            mean(df.speed).alias("mean_speed"),
            variance(df.speed).alias("variance_speed"),
            stddev(df.speed).alias("stddev_speed"),
            min(df.speed).alias("min_speed"),
            max(df.speed).alias("max_speed")
        )
    elif (long1 == None) & (long2 == None) & (lat1 == None) & (lat2 == None) & (date1 != None) & (date2 != None):
        df_ret = df.where(
            (df.datetime > date1) &
            (df.datetime < date2)
        ).agg(
            mean(df.speed).alias("mean_speed"),
            variance(df.speed).alias("variance_speed"),
            stddev(df.speed).alias("stddev_speed"),
            min(df.speed).alias("min_speed"),
            max(df.speed).alias("max_speed")
        )
    return df_ret

def VratiTraseKojeSeKrecuIspodBrzine(df,speed, long1=None, long2=None, lat1=None, lat2=None, date1=None, date2=None):
    if (long1 != None) & (long2 != None) & (lat1 != None) & (lat2 != None) & (date1 == None) & (date2 == None):
        df_ret = df.where(
            (df.longitude < long1) &
            (df.longitude > long2) &
            (df.latitude < lat1) &
            (df.latitude > lat2) &
            (df.speed < speed)
        ).select("busLine", "datetime", "speed")

    elif (long1 != None) & (long2 != None) & (lat1 != None) & (lat2 != None) & (date1 != None) & (date2 != None):
        df_ret = df.where(
            (df.longitude < long1) &
            (df.longitude > long2) &
            (df.latitude < lat1) &
            (df.latitude > lat2) &
            (df.datetime > date1) &
            (df.datetime < date2) &
            (df.speed < speed)
        ).select("busLine", "datetime", "speed")

    elif (long1 == None) & (long2 == None) & (lat1 == None) & (lat2 == None) & (date1 != None) & (date2 != None):
        df_ret = df.where(
            (df.datetime > date1) &
            (df.datetime < date2) &
            (df.speed < speed)
        ).select("busLine", "datetime", "speed")
    return df_ret

def VratiTraseKojeSeKrecuIznadBrzine(df,speed, long1=None, long2=None, lat1=None, lat2=None, date1=None, date2=None):
    if (long1 != None) & (long2 != None) & (lat1 != None) & (lat2 != None) & (date1 == None) & (date2 == None):
        df_ret = df.where(
            (df.longitude < long1) &
            (df.longitude > long2) &
            (df.latitude < lat1) &
            (df.latitude > lat2) &
            (df.speed > speed)
        ).select("busLine", "datetime", "speed")

    elif (long1 != None) & (long2 != None) & (lat1 != None) & (lat2 != None) & (date1 != None) & (date2 != None):
        df_ret = df.where(
            (df.longitude < long1) &
            (df.longitude > long2) &
            (df.latitude < lat1) &
            (df.latitude > lat2) &
            (df.datetime > date1) &
            (df.datetime < date2) &
            (df.speed > speed)
        ).select("busLine", "datetime", "speed")

    elif (long1 == None) & (long2 == None) & (lat1 == None) & (lat2 == None) & (date1 != None) & (date2 != None):
        df_ret = df.where(
            (df.datetime > date1) &
            (df.datetime < date2) &
            (df.speed > speed)
        ).select("busLine", "datetime", "speed")
    return df_ret

def VratiTraseKojeSeKrecuBrzinom(df,speed, long1=None, long2=None, lat1=None, lat2=None, date1=None, date2=None):
    if (long1 != None) & (long2 != None) & (lat1 != None) & (lat2 != None) & (date1 == None) & (date2 == None):
        df_ret = df.where(
            (df.longitude < long1) &
            (df.longitude > long2) &
            (df.latitude < lat1) &
            (df.latitude > lat2) &
            (df.speed == speed)
        ).select("busLine", "datetime", "speed")

    elif (long1 != None) & (long2 != None) & (lat1 != None) & (lat2 != None) & (date1 != None) & (date2 != None):
        df_ret = df.where(
            (df.longitude < long1) &
            (df.longitude > long2) &
            (df.latitude < lat1) &
            (df.latitude > lat2) &
            (df.datetime > date1) &
            (df.datetime < date2) &
            (df.speed == speed)
        ).select("busLine", "datetime", "speed")

    elif (long1 == None) & (long2 == None) & (lat1 == None) & (lat2 == None) & (date1 != None) & (date2 != None):
        df_ret = df.where(
            (df.datetime > date1) &
            (df.datetime < date2) &
            (df.speed == speed)
        ).select("busLine", "datetime", "speed")
    return df_ret

def izvrsenjeSirinaIDuzina(df):

    long1, long2, lat1, lat2 = float(sys.argv[1]), float(sys.argv[2]), float(sys.argv[3]), float(sys.argv[4])

    print("Statističke vrednosti za slučaj kada su prosleđene samo širina i dužina")
    df_statistika = VratiStatistickeVrednosti(df, long1=long1, long2=long2, lat1=lat1, lat2=lat2).show()

    print("Prosečne brzine autobuskih linija za zadatu širinu i dužinu:")
    df_prosek_po_trasi = VratiProsecneBrzinePoTrasi(df, long1=long1, long2=long2, lat1=lat1, lat2=lat2).show()

    #Vratiti vreme kad je brzina mnogo mala
    print("Autobuske linije koje se kreću sporo za prosleđene argumente")
    VratiTraseKojeSeKrecuIspodBrzine(df, speed=10, long1 = long1, long2 = long2, lat1 = lat1, lat2 = lat2).show()

    print("Autobuske linije koje se kreću brzo za prosleđene argumente")
    VratiTraseKojeSeKrecuIznadBrzine(df, speed=50, long1=long1, long2=long2, lat1=lat1, lat2=lat2).show()

def izvrsenjeVreme(df):
    #Preuzmem prvu i poslednju vrednost datuma iz dataframe-a i na te dve vrednosti konkateniram vreme
    date1 = df.head().datetime
    date2 = df.tail(1)[0].datetime

    date1, date2 = InicijalizacijaSamoVremena(date1, date2)

    print("Statističke vrednosti za slučaj kada su prosleđeni samo dani od argumenata")
    df_statistika = VratiStatistickeVrednosti(df, date1=date1, date2=date2).show()

    print("Prosečne brzine autobuskih linija za zadato vreme:")
    df_prosek_po_trasi = VratiProsecneBrzinePoTrasi(df, date1=date1, date2=date2).show()

    print("Autobusi sledećih linija se ne kreću (brzina im je 0) na sledećim lokacijama:")
    VratiTraseKojeSeKrecuBrzinom(df, speed=0, date1=date1, date2=date2).show()

    print("Lokacije autobusa čija brzina prelazi 100 Km/h:")
    VratiTraseKojeSeKrecuIznadBrzine(df, speed=100, date1=date1, date2=date2).show()

def izvrsenjeDatum(df):
    date1, date2 = InicijalizacijaSamoDatuma()

    print("Statističke vrednosti za slučaj kada su prosleđeni samo dani od argumenata")
    df_statistika = VratiStatistickeVrednosti(df, date1=date1, date2=date2).show()

    print("Prosečne brzine autobuskih linija za zadato vreme:")
    df_prosek_po_trasi = VratiProsecneBrzinePoTrasi(df, date1=date1, date2=date2).show()

    print("Autobusi sledećih linija se ne kreću (brzina im je 0) na sledećim lokacijama:")
    VratiTraseKojeSeKrecuBrzinom(df, speed=0, date1=date1, date2=date2).show()

    print("Lokacije autobusa čija brzina prelazi 100 Km/h:")
    VratiTraseKojeSeKrecuIznadBrzine(df, speed=100, date1=date1, date2=date2).show()

def izvrsenjeDatumIVreme(df):
    date1, date2 = InicijalizacijaDatumaIVremena()

    print("Statističke vrednosti za slučaj kada su prosleđeni samo datum i vreme:")
    df_statistika = VratiStatistickeVrednosti(df, date1=date1, date2=date2).show()

    print("Prosečne brzine autobuskih linija za zadato vreme:")
    df_prosek_po_trasi = VratiProsecneBrzinePoTrasi(df, date1=date1, date2=date2).show()

    print("Autobusi sledećih linija se ne kreću (brzina im je 0) na sledećim lokacijama:")
    VratiTraseKojeSeKrecuBrzinom(df, speed=0, date1=date1, date2=date2).show()

    print("Lokacije autobusa čija brzina prelazi 100 Km/h:")
    VratiTraseKojeSeKrecuIznadBrzine(df, speed=100, date1=date1, date2=date2).show()

def izvrsenjeSve(df):
    date1, date2 = InicijalizacijaDatumaIVremena()
    long1, long2, lat1, lat2 = float(sys.argv[1]), float(sys.argv[2]), float(sys.argv[3]), float(sys.argv[4])

    print("Statističke vrednosti za slučaj kada je prosleđeno sve od argumenata")
    df_statistika = VratiStatistickeVrednosti(df, long1=long1, long2=long2, lat1=lat1, lat2=lat2, date1=date1, date2=date2).show()

    print("Prosečne brzine autobuskih linija za sve prosleđene parametre:")
    df_prosek_po_trasi = VratiProsecneBrzinePoTrasi(df, long1=long1, long2=long2, lat1=lat1, lat2=lat2, date1=date1, date2=date2).show()

    print("Autobusi sledećih linija se ne kreću (brzina im je 0) na sledećim lokacijama:")
    VratiTraseKojeSeKrecuBrzinom(df, speed=0, long1=long1, long2=long2, lat1=lat1, lat2=lat2, date1=date1, date2=date2).show()

    print("Lokacije autobusa čija brzina prelazi 100 Km/h:")
    VratiTraseKojeSeKrecuIznadBrzine(df, speed=100, long1=long1, long2=long2, lat1=lat1, lat2=lat2, date1=date1, date2=date2).show()


if __name__ == '__main__':

    brojArgumenata = len(sys.argv)

    if brojArgumenata < 2:
        print("Usage: main.py <input folder> ")
        exit(-1)

    if (brojArgumenata != 3) & (brojArgumenata != 5)  & (brojArgumenata != 9):
        print("Lose prosledjeni argumenti")
        exit(-1)

    if brojArgumenata == 3:
        #Ili dani ili vreme
        if (":" in sys.argv[1]) & (":" in sys.argv[2]):
            df = Inicijalizacija()
            izvrsenjeVreme(df)
        elif (isfloat(sys.argv[1])) & (isfloat(sys.argv[2])):
            df = Inicijalizacija()
            izvrsenjeDatum(df)
        else:
            print("Lose prosledjeni argumenti")
            exit(-1)
    elif brojArgumenata == 5:
        #Ili sirina i duzina, ili datum i vreme
        if (isfloat(sys.argv[1])) & (isfloat(sys.argv[2])) & (":" in sys.argv[3]) & (":" in sys.argv[4]):
            df = Inicijalizacija()
            izvrsenjeDatumIVreme(df)
        elif (isfloat(sys.argv[1])) & (isfloat(sys.argv[2])) & (isfloat(sys.argv[3])) & (isfloat(sys.argv[4])):
            df = Inicijalizacija()
            izvrsenjeSirinaIDuzina(df)
        else:
            print("Lose prosledjeni argumenti")
            exit(-1)
    elif brojArgumenata == 9:
        if (isfloat(sys.argv[1])
                and isfloat(sys.argv[2])
                and isfloat(sys.argv[3])
                and isfloat(sys.argv[4])
                and isfloat(sys.argv[5])
                and isfloat(sys.argv[6])
                and ":" in sys.argv[7]
                and ":" in sys.argv[8]):
            df = Inicijalizacija()
            izvrsenjeSve(df)
        else:
            print("Lose prosledjeni argumenti")
    else:
        print("Lose prosledjeni argumenti")
        exit(-1)
