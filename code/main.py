import base64

from urllib.request import urlopen
from bs4 import BeautifulSoup
import pandas as pd
import datetime as datetime
from bokeh.plotting import figure, show, output_file, save
from bokeh.layouts import column
from bokeh.models import CDSView, GroupFilter, ColumnDataSource, RangeTool, HoverTool
from google.cloud import storage
from google.cloud import bigquery

def crawler(product,url):
  header = ["date","release_type","description"]
  releases = []
  html = urlopen(url)
  pot = BeautifulSoup(html,'lxml')
  bowl = pot.find('section',attrs={"class":"releases"})
  spoons = bowl.findAll(['h2','div','p','ul'])
  for spoon in spoons:
    if spoon.name == 'h2':
      keep_date = spoon.text #get('data-text')
    if spoon.name == 'div':
      keep_type = spoon.get('class')[0]
      keep_disc = spoon.findNext('p').text
      releases.append([keep_date,keep_type,keep_disc])
  df = pd.DataFrame(releases,columns=header)
  df.date = df.date.apply(lambda x: datetime.datetime.strptime(x,"%B %d, %Y")) 
  df['product'] = product
  releasemap = {"release-fixed":"Fix","release-issue":"Issue","release-changed":"Change","release-feature":"Feature","release-announcement":"Announcement"}
  df['release_type'].replace(releasemap, inplace=True)
  return df

def write_bq(df):
  # parameters
  BQ_PROJECT = 'statmike-internal-site'
  BQ_DATASET = 'RELEASE_NOTES'
  BQ_TABLE = 'BQ_Release_Notes'
  BQ_REGION = 'us-central1'
  # client for bq
  bq_client = bigquery.Client(project=BQ_PROJECT)
  # look at datasets, create if needed
  datum = list(bq_client.list_datasets())
  if BQ_DATASET not in datum:
    dataset = bigquery.Dataset(bigquery.dataset.DatasetReference(BQ_PROJECT, BQ_DATASET))
    dataset.location = BQ_REGION
    dataset = bq_client.create_dataset(dataset)
  # create or replace table
  bq_job_config = bigquery.LoadJobConfig(schema=[],write_disposition="WRITE_TRUNCATE",)
  bq_job = bq_client.load_table_from_dataframe(df, "{}.{}.{}".format(BQ_PROJECT,BQ_DATASET,BQTABLE),job_config=bq_job_config)


def bq_plotter():
  pdict = {"bq":"BigQuery","bqml":"BigQuery ML","bqbi":"BigQuery BI Engine","bqdt":"BigQuery Data Transfer Service"}
  bq = crawler('bq','https://cloud.google.com/bigquery/docs/release-notes')
  bqml = crawler('bqml','https://cloud.google.com/bigquery-ml/docs/release-notes')
  bqbi = crawler('bqbi','https://cloud.google.com/bi-engine/docs/release-notes')
  bqdt = crawler('bqdt','https://cloud.google.com/bigquery-transfer/docs/release-notes')

  df = pd.concat([bq,bqml,bqbi,bqdt], axis=0, ignore_index=True)
  df = df.sort_values(by=['date'], ascending=False)

  # update the BQ table with this info - replaces
  write_bq(df)

  colormap = {"bq":"#4285F4", "bqml":"#EA4335", "bqbi":"#FBBC04", "bqdt":"#34A853"}
  source = ColumnDataSource(data=dict(date=df['date'], release=df['release_type'], tip=df['description'],
                                      product=df['product'], productname=[pdict[x] for x in df['product']],
                                      colors=[colormap[x] for x in df['product']]))
  ycats = df.release_type.unique()

  tooltips = [("","@product"),("","@tip")]

  # main plot
  p = figure(title="Big Query Release Notes",
            plot_height=300, plot_width=800, tools="xpan", toolbar_location=None,
            x_axis_type="datetime", x_axis_location="above", tooltips=[("","@productname"),("","@tip")],
            background_fill_color="#F8F9FA", y_range=ycats, x_range=(df.date[100], df.date[0]))
  p.yaxis.axis_label = 'Release Type'

  #loop over products and display glyphs (circles), use CDSView to create view of product subset from source
  for prod in df['product'].unique():
    view = CDSView(source=source, filters=[GroupFilter(column_name='product', group=prod)])
    p.circle('date','release',source=source, view=view, line_color=None, size=10, fill_color='colors', legend_label=pdict[prod])

  p.legend.location='top_left'
  p.legend.click_policy="hide"

  # selection tool
  select = figure(title="Drag the middle and edges of the selection box to change the range above",
                  plot_height=130, plot_width=800, y_range=p.y_range,
                  x_axis_type="datetime", y_axis_type=None,
                  tools="", toolbar_location=None, background_fill_color="#F8F9FA")
  range_tool = RangeTool(x_range=p.x_range)
  range_tool.overlay.fill_color = "#5F6368"
  range_tool.overlay.fill_alpha = 0.2
  select.circle('date', 'release', source=source, line_color=None, fill_color='colors')
  select.ygrid.grid_line_color = None
  select.add_tools(range_tool)
  select.toolbar.active_multi = range_tool

  full = column(p,select)
  output_file("/tmp/bqplot.html")
  save(full)

  storage_client = storage.Client(project='statmike-internal-site')
  bucket = storage_client.get_bucket('statmike-internal-site')
  blob = bucket.blob('bq-timeline/bqplot.html')
  blob.upload_from_filename('/tmp/bqplot.html')
  blob.make_public()


def bq_timeline(event, context):
    """Triggered from a message on a Cloud Pub/Sub topic.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    pubsub_message = base64.b64decode(event['data']).decode('utf-8')
    print(pubsub_message)
    
    bq_plotter()


