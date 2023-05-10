import argparse
from PipelineOptimized import Pipeline

if __name__ == '__main__':
    parser = argparse.ArgumentParser(formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument('--file', required=True, help="data file")
    parser.add_argument('--temp', required=True, help="temp file")
    parser.add_argument('--speaker', required=True, help="speaker : EMacron, EBorne, FHollande, JCastex, JMBlanquer, NSarkozy, PParties")
    parser.add_argument('--debug', required=False, action='store_true', help="more logs")
    parser.add_argument('--reuse', required=False, action='store_true', help="quotes_already_extracted")
    args = parser.parse_args()

    launch = Pipeline(args.file, args.speaker, args.temp, debug=args.debug, quotes_already_extracted=args.reuse)
