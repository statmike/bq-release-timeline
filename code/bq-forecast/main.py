import base64

# import packages
import pandas as pd
import numpy as np

from google.cloud import storage
from google.cloud import bigquery
from google.cloud import bigquery_storage

from bokeh.plotting import figure, show, output_file, save
from bokeh.layouts import column
from bokeh.models import CDSView, GroupFilter, ColumnDataSource, RangeTool, HoverTool


def bq_data(PROJECT,DATASET,TABLE,REGION):
    # setup clients
    bq_client = bigquery.Client(project=PROJECT)
    bq_storage = bigquery_storage.BigQueryReadClient()

    # prepare data for forecasting
    view = bq_client.query(
        """
        CREATE VIEW IF NOT EXISTS `statmike-internal-site.RELEASE_NOTES.Analysis_Data` AS
        WITH PREP AS(SELECT date, product, count(*) as releases
            FROM `statmike-internal-site.RELEASE_NOTES.BQ_Release_Notes`
            GROUP BY date, product
            ORDER BY date, product)
        SELECT date, product, releases, DATE_DIFF(LEAD(date) OVER (PARTITION BY product ORDER BY date),date,DAY) as days_until_next_release
        FROM PREP;
        """
    )
    for row in view: print(view)

    # train the forecast with BQ ML ARIMA-PLUS
    model = bq_client.query(
        """
        CREATE OR REPLACE MODEL `statmike-internal-site.RELEASE_NOTES.arima_plus`
        OPTIONS(MODEL_TYPE='ARIMA_PLUS',
                time_series_timestamp_col='date',
                time_series_data_col='days_until_next_release',
                time_series_id_col='product',
                horizon=20) AS
        SELECT date, days_until_next_release, product
        FROM `statmike-internal-site.RELEASE_NOTES.Analysis_Data`;
        """
    )
    for row in model: print(row)

    # retrieve forecast
    forecast = bq_client.query(
        """
        WITH
        FORECAST AS(
            WITH
            RETRIEVE AS(
                SELECT *
                FROM ML.EXPLAIN_FORECAST(MODEL `statmike-internal-site.RELEASE_NOTES.arima_plus`,STRUCT(10 AS horizon, 0.8 AS confidence_level))
                WHERE time_series_type='forecast'
            )
            SELECT *
            FROM RETRIEVE
            WHERE DATE(time_series_timestamp) < CURRENT_DATE()),
        MATCH AS(
            SELECT product, MAX(DATE(time_series_timestamp)) as match_date
            FROM FORECAST
            GROUP by product
            HAVING match_date < CURRENT_DATE()
        )
        SELECT *
        FROM
        (SELECT * FROM MATCH) a 
        LEFT OUTER JOIN
        (SELECT *, DATE(time_series_timestamp) as match_date FROM FORECAST) b
        ON a.product = b.product and a.match_date = b.match_date
        """
    )
    forecast = forecast.to_dataframe()

    # add plot friendly info to the forecast
    forecast['release_type'] = 'Forecasted Release Note'
    forecast['description'] = 'Forecasted Timerange for Next Release Note'
    forecast['target'] = forecast['time_series_timestamp']+pd.to_timedelta(forecast['time_series_data'],unit='d')
    forecast['targetL'] = forecast['time_series_timestamp']+pd.to_timedelta(forecast['prediction_interval_lower_bound'],unit='d')
    forecast['targetU'] = forecast['time_series_timestamp']+pd.to_timedelta(forecast['prediction_interval_upper_bound'],unit='d')

    # download release notes
    parent = f"projects/{PROJECT}"
    table = f"{parent}/datasets/{DATASET}/tables/{TABLE}"

    requested_session = bigquery_storage.types.ReadSession(table=table, data_format=bigquery_storage.types.DataFormat.ARROW)
    read_session = bq_storage.create_read_session(parent=parent, read_session=requested_session, max_stream_count=1,)
    stream = read_session.streams[0]
    reader = bq_storage.read_rows(stream.name)
    df = reader.to_dataframe(read_session)

    return df, forecast

# create bokeh plot
def bq_plotter(PROJECT,DATASET,TABLE,REGION):
    # data
    df, forecast = bq_data(PROJECT,DATASET,TABLE,REGION)

    # sort the data - this impacts the range selection tool which used indexes
    df = df.sort_values(by=['date'], ascending=False, ignore_index=True)

    # dictionaries for poduct labels and colors
    pdict = {"bq":"BigQuery","bqml":"BigQuery ML","bqbi":"BigQuery BI Engine","bqdt":"BigQuery Data Transfer Service"}
    colormap = {"bq":"#4285F4", "bqml":"#EA4335", "bqbi":"#FBBC04", "bqdt":"#34A853"}

    # define the source data for the plot
    source = ColumnDataSource(data=dict(date=df['date'], release=df['release_type'], tip=df['description'],
                                        product=df['product'], productname=[pdict[x] for x in df['product']],
                                        colors=[colormap[x] for x in df['product']]))
    ycats = df.release_type.unique()
    ycats = np.append(ycats,forecast['release_type'][0])

    # define the source data for the forecast
    sourceF = ColumnDataSource(data=dict(release=forecast['release_type'], product=forecast['product'], productnameF=[pdict[x] for x in forecast['product']],
                                        target=forecast['target'], targetL=forecast['targetL'], targetU=forecast['targetU'], tipF=forecast['description'], 
                                        colors=[colormap[x] for x in forecast['product']]))

    # format date for tooltips
    source.add(df['date'].apply(lambda d: d.strftime('%m/%d/%Y')),'date_pretty')
    sourceF.add(forecast['target'].apply(lambda d: d.strftime('%m/%d/%Y')),'date_pretty_target')
    sourceF.add(forecast['targetL'].apply(lambda d: d.strftime('%m/%d/%Y')),'date_pretty_targetL')
    sourceF.add(forecast['targetU'].apply(lambda d: d.strftime('%m/%d/%Y')),'date_pretty_targetU')

    # create hover tools
    p_hover = HoverTool(mode='mouse', line_policy='nearest', names=['p_primary'],
                        tooltips=[("","@date_pretty @productname"),("","@tip")])
    target_hover = HoverTool(mode='mouse', line_policy='nearest', names=['p_target'],
                        tooltips=[("Forecast:","@date_pretty_target"),("Product:", "@productnameF"),("Note","@tipF"),("Lower Prediction (80%): ","@date_pretty_targetL"),("Upper Prediction (80%): ","@date_pretty_targetU")])


    # main plot = p
    p = figure(title="Big Query Release Notes With Forecasted Next Note",
            plot_height=300, plot_width=800, tools=["xpan",p_hover,target_hover], toolbar_location=None,
            x_axis_type="datetime", x_axis_location="above", 
            background_fill_color="#F8F9FA", y_range=ycats, x_range=(df.date[100], forecast.targetU.max()))
    p.yaxis.axis_label = 'Release Type'

    #loop over products and display glyphs (circles), use CDSView to create view of product subset from source
    for prod in df['product'].unique():
        view = CDSView(source=source, filters=[GroupFilter(column_name='product', group=prod)])
        p.circle('date','release',source=source, view=view, line_color=None, size=10, fill_color='colors', legend_label=pdict[prod], name='p_primary')
        # add forecast data
        viewF = CDSView(source=sourceF, filters=[GroupFilter(column_name='product', group=prod)])
        p.circle('target','release',source=sourceF, view=viewF, line_color=None, size=10, fill_color='colors', name='p_target')

    # configure legend
    p.legend.location='top_left'
    p.legend.click_policy="hide"

    # selection tool below chart = select
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

    # save to gcs bucket and make public
    gcs_client = storage.Client(project=PROJECT)
    bucket = gcs_client.get_bucket(PROJECT)
    blob = bucket.blob('bq-forecast/bqplot.html')
    blob.upload_from_filename('/tmp/bqplot.html')
    blob.make_public()

def bq_forecast(event, context):
    """Triggered from a message on a Cloud Pub/Sub topic.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    pubsub_message = base64.b64decode(event['data']).decode('utf-8')
    print(pubsub_message)
    
    # setup parameters
    PROJECT = 'statmike-internal-site'
    DATASET = 'RELEASE_NOTES'
    TABLE = 'BQ_Release_Notes'
    REGION = 'us-central1'

    bq_plotter(PROJECT,DATASET,TABLE,REGION)