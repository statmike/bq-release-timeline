# Create A Timeline of BigQuery Release Notes

This project creates [this plot](https://storage.googleapis.com/statmike-internal-site/bq-timeline/bqplot.html).

Then it uses [Big Query ML](https://cloud.google.com/bigquery-ml/docs/introduction) to train a forecast and creates [this plot]().

## Code Walkthrough with Colab

This [Colab](https://gist.github.com/statmike/e87ba9b4c3e810970af7c87bf2d17662) is an interactive walkthrough of the timeline creation:
- Crawl the BigQuery Release Notes [pages](https://cloud.google.com/bigquery/docs/release-notes) using [urllib](https://docs.python.org/3/library/urllib.html)
- Parse the html with [BeautifulSoup](https://www.crummy.com/software/BeautifulSoup/)
- create a BigQuery table to store the timelines (used in other projects)
- Create a [Bokeh](https://docs.bokeh.org/en/latest/index.html) figure

This [Colab](https://gist.github.com/statmike/c64f1f7883270044ed70f093f74e6738) is an interactive walkthrough of the forecast creation:

## Automate with a Google Cloud Function

Use [Google Cloud](https://cloud.google.com/)! 
- Create a [Cloud Function](https://cloud.google.com/functions)
    - That pulls source code from a [Cloud Source Repository](https://cloud.google.com/source-repositories)
        - That is a mirror of a [public repository on GitHub](https://github.com/statmike/bq-release-timeline)
- The function subscribes to a topic on [Pub/Sub](https://cloud.google.com/pubsub)
- A job that invokes the Pub/Sub topic is created with [Cloud Scheduler](https://cloud.google.com/scheduler)
- The function write results to a bucket in [Cloud Storage](https://cloud.google.com/storage)
- And the results are [set to public](https://cloud.google.com/storage/docs/access-control/making-data-public) so a URL can be used to access from anywhere
