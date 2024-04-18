<show-structure for="chapter,procedure" depth="2"/>

# Aggregations reference

Astra supports a variety of search aggregations which fall into three primary categories: metric aggregations, 
bucket aggregations, and pipeline aggregations.

## Metric aggregations

Metric aggregations typically operate on single or multiple value fields and allow you to do simple field calculations,
such as finding the average value.

### Average

<deflist type="medium">
<def title="field (required)">Sets the field to use for this aggregation.</def>
<def title="script (optional)">Sets the script to use for this aggregation.</def>
<def title="missing (optional)">Sets the value to use when the aggregation finds a missing value in a document.</def>
</deflist>

<tabs>
<tab title="Example JSON Request">

```json

{
  "aggs": {
    "example_avg": {
      "avg": {
        "field": "duration_ms"
      }
    }
  }
}
```
</tab>
<tab title="Example JSON Response">

```json

{
  "aggregations": {
    "example_avg": {
      "value": 101.22
    }
  }
}
```
</tab>
</tabs>

### Extended stats

<deflist type="medium">
<def title="field (required)">Sets the field to use for this aggregation.</def>
<def title="sigma (optional)">Sets the sigma to use for this aggregation.</def>
<def title="script (optional)">Sets the script to use for this aggregation.</def>
<def title="missing (optional)">Sets the value to use when the aggregation finds a missing value in a document.</def>
</deflist>

<tabs>
<tab title="Example JSON Request">

```json

{
  "aggs": {
    "example_extended_stats": {
      "extended_stats": {
        "field": "duration_ms"
      }
    }
  }
}
```
</tab>
<tab title="Example JSON Response">

```json

{
  "aggregations": {
    "example_extended_stats": {
      "count": 27985332,
      "min": 1,
      "max": 9208605921000,
      "avg": 792442.8374768253,
      "sum": 22176775897811,
      "sum_of_squares": 8.479931167082962e+25,
      "variance": 3030133574867091000,
      "variance_population": 3030133574867091000,
      "variance_sampling": 3030133683142872000,
      "std_deviation": 1740727886.5081387,
      "std_deviation_population": 1740727886.5081387,
      "std_deviation_sampling": 1740727917.6088583,
      "std_deviation_bounds": {
        "upper": 3482248215.853754,
        "lower": -3480663330.1788006,
        "upper_population": 3482248215.853754,
        "lower_population": -3480663330.1788006,
        "upper_sampling": 3482248278.0551934,
        "lower_sampling": -3480663392.38024
      }
    }
  }
}
```
</tab>
</tabs>

### Max

<deflist type="medium">
<def title="field (required)">Sets the field to use for this aggregation.</def>
<def title="script (optional)">Sets the script to use for this aggregation.</def>
<def title="missing (optional)">Sets the value to use when the aggregation finds a missing value in a document.</def>
</deflist>

<tabs>
<tab title="Example JSON Request">

```json

{
  "aggs": {
    "example_max": {
      "max": {
        "field": "duration_ms"
      }
    }
  }
}
```
</tab>
<tab title="Example JSON Response">

```json

{
  "aggregations": {
    "example_max": {
      "value": 1288.8273
    }
  }
}
```
</tab>
</tabs>

### Min

<deflist type="medium">
<def title="field (required)">Sets the field to use for this aggregation.</def>
<def title="script (optional)">Sets the script to use for this aggregation.</def>
<def title="missing (optional)">Sets the value to use when the aggregation finds a missing value in a document.</def>
</deflist>

<tabs>
<tab title="Example JSON Request">

```json

{
  "aggs": {
    "example_min": {
      "min": {
        "field": "duration_ms"
      }
    }
  }
}
```
</tab>
<tab title="Example JSON Response">

```json

{
  "aggregations": {
    "example_min": {
      "value": 8.8273
    }
  }
}
```
</tab>
</tabs>

### Percentiles

<deflist type="medium">
<def title="field (required)">Sets the field to use for this aggregation.</def>
<def title="percents (optional)">

Set the values to compute percentiles from (ie, `[50, 95, 99, 99.9.]`).
</def>
<def title="script (optional)">Sets the script to use for this aggregation.</def>
<def title="missing (optional)">Sets the value to use when the aggregation finds a missing value in a document.</def>
</deflist>

<tabs>
<tab title="Example JSON Request">

```json

{
  "aggs": {
    "example_percentiles": {
      "percentiles": {
        "field": "duration_ms"
      }
    }
  }
}
```
</tab>
<tab title="Example JSON Response">

```json

{
  "aggregations": {
    "example_percentiles": {
      "values" : {
        "1.0" : 2.881,
        "5.0" : 27.984375,
        "25.0" : 87.99871,
        "50.0" : 128.99,
        "75.0" : 507.8872,
        "95.0" : 778.98,
        "99.0" : 808.1112
      }
    }
  }
}
```
</tab>
</tabs>

### Sum

<deflist type="medium">
<def title="field (required)">Sets the field to use for this aggregation.</def>
<def title="script (optional)">Sets the script to use for this aggregation.</def>
<def title="missing (optional)">Sets the value to use when the aggregation finds a missing value in a document.</def>
</deflist>

<tabs>
<tab title="Example JSON Request">

```json

{
  "aggs": {
    "example_sum": {
      "sum": {
        "field": "duration_ms"
      }
    }
  }
}
```
</tab>
<tab title="Example JSON Response">

```json

{
  "aggregations": {
    "example_sum": {
      "value": 19287878.8273818272
    }
  }
}
```
</tab>
</tabs>

### Unique count

<tip>

Also known as `cardinality`.
</tip>

<deflist type="medium">
<def title="field (required)">Sets the field to use for this aggregation.</def>
<def title="precision_threshold (optional)">Set a precision threshold. Higher values improve accuracy but also increase 
memory usage.</def>
<def title="missing (optional)">Sets the value to use when the aggregation finds a missing value in a document.</def>
</deflist>

<tabs>
<tab title="Example JSON Request">

```json

{
  "aggs": {
    "example_unique_count": {
      "cardinality": {
        "field": "trace_id"
      }
    }
  }
}
```
</tab>
<tab title="Example JSON Response">

```json

{
  "aggregations": {
    "example_unique_count": {
      "value": 19982
    }
  }
}
```
</tab>
</tabs>

## Bucket aggregations

Bucket aggregations categorize the results into common groupings known as buckets. These groupings are based on the 
field values of individual results, and include date and value based aggregations.

### Date histogram

<deflist type="full">
<def title="field (required)">Sets the field to use for this aggregation.</def>
<def title="min_doc_count (required)">Set the minimum count of matching documents that buckets need to have.</def>
<def title="interval (required)">Sets the field to use for this aggregation.</def>

<def title="offset (optional)">Set the offset on this builder, as a time value (1m, 10m).</def>
<def title="time_zone (optional)">Sets the time zone to use for this aggregation.</def>
<def title="extended_bounds (optional)">
Set extended bounds on this histogram, so that buckets would also be generated on intervals that did not match any 
documents.
<deflist type="wide">
<def title="min">Minimum epoch in milliseconds</def>
<def title="max">Maximum epoch in milliseconds</def>
</deflist>
</def>
<def title="buckets (optional)">

Sets the number of buckets to return when using an `auto` interval.  
</def>
<def title="minimum_interval (optional)">

Sets the minimum interval expression when using an `auto` interval.
</def>
</deflist>

<tabs>
<tab title="Example JSON Request">

```json

{
  "aggs": {
    "example_date_histogram": {
      "date_histogram": {
        "interval": "20m",
        "field": "_timesinceepoch",
        "min_doc_count": 0,
        "extended_bounds": {
          "min": 1713381276539,
          "max": 1713384876540
        },
        "format": "epoch_millis"
      }
    }
  }
}
```
</tab>
<tab title="Example JSON Response">

```json

{
  "aggregations": {
    "example_date_histogram": {
      "buckets": [{
          "key": 1713380400000,
          "doc_count": 410997923
        }, {
          "key": 1713381600000,
          "doc_count": 1607153025
        },{
          "key": 1713382800000,
          "doc_count": 1614948047
        }, {
          "key": 1713384000000,
          "doc_count": 1144936770
        }
      ]
    }
  }
}
```
</tab>
</tabs>

### Filters

<deflist type="full">
<def title="filters (required)">
List of keyed filters to use for this aggregation. 

<deflist type="full">
<def title="{filter key} (required)">
<deflist type="wide">
<def title="query (required)">
Query string to use for this keyed filter.
</def>
<def title="analyze_wildcard (required)">

Set to `true` to enable analysis on wildcard and prefix queries.
</def>
</deflist>
</def>
</deflist>
</def>
</deflist>

<tabs>
<tab title="Example JSON Request">

```json

{
  "aggs": {
    "example_filters": {
      "filters": {
        "filters": {
          "over_10k": {
            "query_string": {
              "query": "duration:>10000",
              "analyze_wildcard": true
            }
          },
          "under_10k": {
            "query_string": {
              "query": "duration:<10000",
              "analyze_wildcard": true
            }
          }
        }
      }
    }
  }
}
```
</tab>
<tab title="Example JSON Response">

```json

{
  "aggregations": {
    "example_filters": {
      "buckets": {
        "over_10k": {
          "doc_count": 142201162
        },
        "under_10k": {
          "doc_count": 220037018
        }
      }
    }
  }
}
```
</tab>
</tabs>

### Histogram

<deflist type="medium">
<def title="field (required)">Sets the field to use for this aggregation.</def>
<def title="min_doc_count (required)">Set the minimum count of matching documents that buckets need to have.</def>
<def title="interval (required)">Sets the interval to use for this aggregation</def>
</deflist>

<tabs>
<tab title="Example JSON Request">

```json

{
  "aggs": {
    "example_histogram": {
      "histogram": {
        "interval": "100000000",
        "field": "duration",
        "min_doc_count": "100"
      }
    }
  }
}
```
</tab>
<tab title="Example JSON Response">

```json

{
  "aggregations": {
    "example_histogram": {
      "buckets": [
        {
          "key": 0,
          "doc_count": 359845511
        },
        {
          "key": 100000000,
          "doc_count": 155679
        },
        {
          "key": 200000000,
          "doc_count": 19991
        }
      ]
    }
  }
}
```
</tab>
</tabs>

### Terms

<deflist type="medium">
<def title="field (required)">Sets the field to use for this aggregation.</def>
<def title="min_doc_count (required)">Set the minimum document count terms should have in order to appear in the 
response.</def>
<def title="size (required)">Indicates how many term buckets should be returned (defaults to 10)</def>
<def title="order (required)">Sets the order in which the buckets will be returned.</def>
<def title="missing (optional)">Sets the value to use when the aggregation finds a missing value in a document.</def>
</deflist>

<tabs>
<tab title="Example JSON Request">

```json

{
  "aggs": {
    "example_terms": {
      "terms": {
        "field": "trace_id",
        "size": 3,
        "order": {
          "_term": "desc"
        },
        "min_doc_count": 0
      }
    }
  }
}
```
</tab>
<tab title="Example JSON Response">

```json

{
  "aggregations": {
    "example_terms": {
      "doc_count_error_upper_bound": 280163,
      "sum_other_doc_count": 1760076441,
      "buckets": [
        {
          "key": "f9ef245a341bc750b1c11dfe0c031526",
          "doc_count": 2157644
        }, {
          "key": "8c75d28135b72bc4a6e76f392eb04166",
          "doc_count": 1021020
        }, {
          "key": "116f77d0319c01d83f85d5bcc65bd2a1",
          "doc_count": 622093
        }
      ]
    }
  }
}
```
</tab>
</tabs>

## Pipeline aggregations

Pipeline aggregations enable nesting multiple aggregations together, feeding the results of one aggregation as the 
input to another aggregation. 

### Cumulative sum

<deflist type="wide">
<def title="buckets path (required)">Path to pipeline aggregation.</def>
<def title="format (optional)">Sets the format to use on the output of this aggregation.</def>
</deflist>

<tabs>
<tab title="Example JSON Request">

```json
{
  "aggs": {
    "example_cumulative_sum_": {
        "date_histogram": {
          "interval": "100ms",
          "field": "_timesinceepoch",
          "min_doc_count": "0",
          "extended_bounds": {
            "min": 1713385504391,
            "max": 1713385804391
          },
          "format": "epoch_millis"
        },
        "aggs": {
          "example_average": {
            "avg": {
              "field": "duration"
            }
          },
          "example_cumulative_sum": {
            "cumulative_sum": {
              "buckets_path": "example_average"
            }
          }
        }
      }
    }
  }
```
</tab>
<tab title="Example JSON Response">

```json

{
  "aggregations": {
    "example_cumulative_sum_": {
      "buckets": [{
          "example_average": {
            "value": 7401858.293426207
          },
          "example_cumulative_sum": {
            "value": 7401858.293426207
          },
          "key": 1713385620000,
          "doc_count": 55148506
        },
        {
          "example_average": {
            "value": 27927921.094381377
          },
          "example_cumulative_sum": {
            "value": 35329779.387807585
          },
          "key": 1713385680000,
          "doc_count": 77229532
        },
        {
          "example_average": {
            "value": 5531517.61740416
          },
          "example_cumulative_sum": {
            "value": 40861297.00521175
          },
          "key": 1713385740000,
          "doc_count": 80424458
        },
        {
          "example_average": {
            "value": 5895626.263929776
          },
          "example_cumulative_sum": {
            "value": 46756923.269141525
          },
          "key": 1713385800000,
          "doc_count": 79971212
        },
        {
          "example_average": {
            "value": 2972519.9794683917
          },
          "example_cumulative_sum": {
            "value": 49729443.248609915
          },
          "key": 1713385860000,
          "doc_count": 69229353
        },
        {
          "example_average": {
            "value": 162020.54769622226
          },
          "example_cumulative_sum": {
            "value": 49891463.79630614
          },
          "key": 1713385920000,
          "doc_count": 9787475
        }
      ]
    }
  }
}
```
</tab>
</tabs>

### Derivative

<deflist type="wide">
<def title="buckets path (required)">Path to pipeline aggregation.</def>
<def title="unit (optional)">Sets the unit to use for this aggregation.</def>
</deflist>

<tabs>
<tab title="Example JSON Request">

```json

{
  "aggs": {
    "example_derivative_": {
      "date_histogram": {
        "interval": "100ms",
        "field": "_timesinceepoch",
        "min_doc_count": "0",
        "extended_bounds": {
          "min": 1713385504391,
          "max": 1713385804391
        },
        "format": "epoch_millis"
      },
      "aggs": {
        "example_average": {
          "avg": {
            "field": "duration"
          }
        },
        "example_derivative": {
          "derivative": {
            "buckets_path": "example_average"
          }
        }
      }
    }
  }
}
```
</tab>
<tab title="Example JSON Response">

```json

{
  "aggregations": {
    "example_derivative_": {
      "buckets": [
        {
          "example_average": {
            "value": 5792060.911828078
          },
          "key": 1713385740000,
          "doc_count": 43804445
        },
        {
          "example_average": {
            "value": 6079661.857464066
          },
          "example_derivative": {
            "value": 287600.94563598745
          },
          "key": 1713385800000,
          "doc_count": 80729821
        },
        {
          "example_average": {
            "value": 5897079.15460301
          },
          "example_derivative": {
            "value": -182582.70286105573
          },
          "key": 1713385860000,
          "doc_count": 79099246
        },
        {
          "example_average": {
            "value": 5782722.0436034165
          },
          "example_derivative": {
            "value": -114357.11099959351
          },
          "key": 1713385920000,
          "doc_count": 77868435
        },
        {
          "example_average": {
            "value": 3943321.660900215
          },
          "example_derivative": {
            "value": -1839400.3827032014
          },
          "key": 1713385980000,
          "doc_count": 70314331
        },
        {
          "example_average": {
            "value": 438287.2007225716
          },
          "example_derivative": {
            "value": -3505034.4601776437
          },
          "key": 1713386040000,
          "doc_count": 19067226
        }
      ]
    }
  }
}
```
</tab>
</tabs>

### Moving average

<deflist type="full">
<def title="buckets path (required)">Path to pipeline aggregation.</def>
<def title="model (required)">

Sets a model for the moving average. The model is used to define what type of moving average you want to use on the 
series, and is one of `simple`, `linear`, `ewma`, `holt`, or `holt_winters`.


<deflist type="medium">
<def title="simple">
Calculate a simple unweighted (arithmetic) moving average.
</def>
<def title="linear">
Calculate a linearly weighted moving average, such that older values are linearly less important. "Time" is determined 
by position in collection.
</def>
<def title="ewma">
Calculate a exponentially weighted moving average.
</def>
<def title="holt">
Calculate a doubly exponential weighted moving average.
</def>
<def title="holt_winters">
Calculate a triple exponential weighted moving average.
</def>
</deflist>
</def>
<def title="window (optional)">
Sets the window size for the moving average. This window will "slide" across the series, and the values inside that 
window will be used to calculate the moving avg value.
</def>
<def title="predict (optional)">
Sets the number of predictions that should be returned. Each prediction will be spaced at the intervals specified in 
the histogram. E. g "predict: 2" will return two new buckets at the end of the histogram with the predicted values.
</def>
</deflist>

<tabs>
<tab title="Example JSON Request">

```json

{
  "aggs": {
    "example_moving_avg_": {
      "date_histogram": {
        "interval": "100ms",
        "field": "_timesinceepoch",
        "min_doc_count": "0",
        "extended_bounds": {
          "min": 1713385504391,
          "max": 1713385804391
        },
        "format": "epoch_millis"
      },
      "aggs": {
        "example_average": {
          "avg": {
            "field": "duration"
          }
        },
        "example_moving_avg": {
          "moving_avg": {
            "buckets_path": "example_average",
            "model": "simple",
            "window": 1,
            "predict": 1
          }
        }
      }
    }
  }
}
```
</tab>
<tab title="Example JSON Response">

```json

{
  "aggregations": {
    "example_moving_function_": {
      "buckets": [{
        "example_average": {
          "value": 6561779.138131231
        },
        "key": 1713385440000,
        "doc_count": 33714165
      }, {
        "example_average": {
          "value": 6485698.418706569
        },
        "example_moving_avg": {
          "value": 6561779.138131231
        },
        "key": 1713385500000,
        "doc_count": 78041366
      }, {
        "example_average": {
          "value": 9782059.847589057
        },
        "example_moving_avg": {
          "value": 6485698.418706569
        },
        "key": 1713385560000,
        "doc_count": 78410185
      }, {
        "example_average": {
          "value": 7474590.152600041
        },
        "example_moving_avg": {
          "value": 9782059.847589057
        },
        "key": 1713385620000,
        "doc_count": 78214305
      }, {
        "example_average": {
          "value": 28057218.56774117
        },
        "example_moving_avg": {
          "value": 7474590.152600041
        },
        "key": 1713385680000,
        "doc_count": 78133058
      }, {
        "example_average": {
          "value": 5965070.914908828
        },
        "example_moving_avg": {
          "value": 28057218.56774117
        },
        "key": 1713385740000,
        "doc_count": 81542251
      }
      ]
    }
  }
}
```
</tab>
</tabs>

### Moving function

<deflist type="wide">
<def title="buckets path (required)">Path to pipeline aggregation</def>
<def title="window (required)">Window size for this aggregation</def>
<def title="script (required)">Sets the Painless script to use for this aggregation.</def>
<def title="shift (optional)">Sets the window shift to use for this aggregation.</def>
</deflist>

<tabs>
<tab title="Example JSON Request">

```json

{
  "aggs": {
    "example_moving_function_": {
      "date_histogram": {
        "interval": "100ms",
        "field": "_timesinceepoch",
        "min_doc_count": "0",
        "extended_bounds": {
          "min": 1713385504391,
          "max": 1713385804391
        },
        "format": "epoch_millis"
      },
      "aggs": {
        "example_average": {
          "avg": {
            "field": "duration"
          }
        },
        "example_moving_function": {
          "moving_fn": {
            "buckets_path": "1",
            "window": "1",
            "script": "MovingFunctions.min(values)"
          }
        }
      }
    }
  }
}
```
</tab>
<tab title="Example JSON Response">

```json
{
  "aggregations": {
    "example_moving_function_": {
      "buckets": [{
          "example_average": {
            "value": 44292399.56775201
          },
          "example_moving_function": {
            "value": null
          },
          "key": 1713385680000,
          "doc_count": 45119587
        }, {
          "example_average": {
            "value": 6174371.77869405
          },
          "example_moving_function": {
            "value": 44292399.56775201
          },
          "key": 1713385740000,
          "doc_count": 81840356
        }, {
          "example_average": {
            "value": 6771376.920665294
          },
          "example_moving_function": {
            "value": 6174371.77869405
          },
          "key": 1713385800000,
          "doc_count": 81786893
        }, {
          "example_average": {
            "value": 6394727.146436909
          },
          "example_moving_function": {
            "value": 6771376.920665294
          },
          "key": 1713385860000,
          "doc_count": 80362159
        }
      ]
    }
  }
}
```
</tab>
</tabs>
