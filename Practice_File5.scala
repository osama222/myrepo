// hdfs dfs -put /downloads/crimes.csv /user/btadmin/crimesdataset
// read dataset
val crimeData = sc.textFile("/user/btadmin/crimesdataset")
// display top 10 records
crimeData.take(10).foreach(println)
// ID,Case Number,Date,Block,IUCR,Primary Type,Description,Location Description,Arrest,Domestic,Beat,District,Ward,Community Area,FBI Code,X Coordinate,Y Coordinate,Year,Updated On,Latitude,Longitude,Location
//4647369,HM155213,01/31/2006 12:13:05 PM,066XX N BOSWORTH AVE,1811,NARCOTICS,POSS: CANNABIS 30GMS OR LESS,"SCHOOL, PUBLIC, BUILDING",true,false,2432,024,40,1,18,1164737,1944193,2006,04/15/2016 08:55:02 AM,42.002478396,-87.66929687,"(42.002478396, -87.66929687)"
// we need to exclude the header from the dataset
val crimeHeader = crimeData.first
val crimeDataWithoutHeader = crimeData.filter(criminalRecord => criminalRecord != crimeHeader)
val rec = crimeDataWithoutHeader.first
val distinctDates = crimeDataWithoutHeader.map(rec => rec.split(",")(2).split(" ")(0)).distinct.collect.sorted
val distinctDates = crimeDataWithoutHeader.map(rec => {
    val r = rec.split(",") // 4647369,HM155213,01/31/2006 12:13:05 PM,066XX N BOSWORTH AVE,1811,NARCOTICS,POSS: CANNABIS 30GMS OR LESS,"SCHOOL, PUBLIC, BUILDING",true,false,2432,024,40,1,18,1164737,1944193,2006,04/15/2016 08:55:02 AM,42.002478396,-87.66929687,"(42.002478396, -87.66929687)"
    val d = r(2).split(" ")(0) 
    val m = d.split("/")(2) + d.split("/")(0)
    ((m.toInt, r(5)), 1)
})

// another way to do it 
val criminalRecordsWithMonthAndType = crimeDataWithoutHeader.map(crimeDate => {
   val cr = crimeDate.split(",")
   ((cr(5),crimeDate.split(",")(2).split(" ")(0).split("/")(2) + crimeDate.split(",")(2).split(" ")(0).split("/")(0)), 1)
})

((NARCOTICS,200601),1)
((CRIMINAL TRESPASS,200603),1)
((NARCOTICS,200602),1)


val crimeCountPerMonthPerType = criminalRecordsWithMonthAndType.reduceByKey((total, value) => total + value)

((HOMICIDE,201311),28)
((DECEPTIVE PRACTICE,200402),977)
((ARSON,200609),61)

// the final shape should look like
// ((200707,WEAPONS VIOLATION),count) -> ((200707, count), "200707,count,WEAPONS VIOLATION")
val crimeCountPerMonthPerTypeSorted = crimeCountPerMonthPerType.map(
   (rec => ((rec._1._2, -rec._2), rec._1._2 + "\t" + rec._2 + "\t" + rec._1._2))
).sortByKey()



