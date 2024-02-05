#!/bin/bash

cd "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)" &&
    python3 preprocess_data.py -n &&
    python3 process_data.py -n &&
    python3 postprocess_data.py -n &&
    python3 measure_features.py -n &&
    python3 measure_indicators.py -n &&
    echo "Finished analyzing data"
