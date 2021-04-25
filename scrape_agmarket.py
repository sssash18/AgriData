
import requests
import pandas as pd
import csv
import psycopg2
import getopt,sys
import selenium
from selenium import webdriver
arguementlist = sys.argv[1:]
options = "c:sd:ed:ta:st:"
longoptions = ["commodity =","start_date =","end_date =","time_agg =","states ="]
try:
    arguments, values = getopt.getopt(arguementlist, options, longoptions)
    commodity = arguments[0][1].capitalize();
    start_date = arguments[1][1];
    end_date = arguments[2][1];
    time_agg = arguments[3][1];
    states = arguments[4][1];
 		
             
except getopt.error as err:
    # output error, and return with an error code
    print (str(err))
#Enter executable path of Chrome Driver and also in system path
browser = webdriver.Chrome(executable_path = "/Users/suyashchoudhary/Desktop/Study/chromedriver")
browser.get("https://agmarknet.gov.in/default.aspx")
select_box = browser.find_element_by_id("ddlCommodity")
options = [x for x in select_box.find_elements_by_tag_name("option")]
comm_no = 0
for element in options:
    if element.get_attribute("label")==commodity:
    	comm_no = element.get_attribute("value")


select_box = browser.find_element_by_id("ddlState")
options = [x for x in select_box.find_elements_by_tag_name("option")]

states_list = states.split(',')



month = {"01":"Jan","02":"Feb","03":"Mar","04":"Apr","05":"May","06":"Jun","07":"Jul","08":"Aug","09":"Sep","10":"Oct","11":"Nov","12":"Dec"}
st_date = start_date.split("-")
st_date_fin = st_date[2] + "-"
st_date_fin += month[st_date[1]]+"-"+st_date[0]


ed_date = end_date.split("-")
ed_date_fin = ed_date[2] + "-"
ed_date_fin += month[ed_date[1]]+"-"+ed_date[0]

for  i in states_list:
	state = 0
	for element in options:
	    if element.get_attribute("label")==i:
	    	state = element.get_attribute("value")

	conn = psycopg2.connect("host=localhost dbname=agriiq user=postgres")
	cur = conn.cursor()
	cur.execute("""DROP TABLE IF EXISTS agmarket_monthly""")
	cur.execute("""CREATE TABLE agmarket_monthly(
			s_no integer PRIMARY KEY,
			State_Name text,
			District_Name text,
			Market_Name text,
			Variety text,
			group_in text,
			Arrivals float,
			Min_Price float,
			Max_Price float,
			Modal_Price float,
			Reported_date text)""")
	conn.commit()
	url = "https://agmarknet.gov.in/SearchCmmMkt.aspx?Tx_Commodity="+str(comm_no)+"&Tx_State="+state+"&Tx_District=0&Tx_Market=0&DateFrom="+str(st_date_fin)+"&DateTo="+str(ed_date_fin)+"&Fr_Date="+str(st_date_fin)+"&To_Date=" + str(ed_date_fin)+"&Tx_Trend=2&Tx_CommodityHead="+str(commodity)+"&Tx_StateHead=--Select--&Tx_DistrictHead=--Select--&Tx_MarketHead=--Select--"
	# print(url)
	timeout = 10
	html = None
	try:
		html = requests.get(url,timeout = timeout)
		html = html.content

	except(requests.ConnectionError,requests.Timeout):
		print("No Internet Connection Or Timeout")
		exit()
	df_list = pd.read_html(html)
	df = df_list[-1]
	df.to_csv('mydata'+i+'.csv')
	with open('mydata'+i+'.csv','r') as f:
		reader = csv.reader(f)
		next(reader)
		for row in reader:
			if row[1]=='-':
				continue
			cur.execute(
				"INSERT INTO agmarket_monthly VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",row
			)

	conn.commit()


