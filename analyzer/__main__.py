from master.component import MasterComponent

from .component import AnalyzerComponent

def main():
    analyzer_component = AnalyzerComponent()
    master_component = MasterComponent()

    producer_observer = master_component.producer_observer()
    analyzer_event_handler = analyzer_component.parallel_disaster_analyzer()
    analyzer_event_handler.start(producer_observer.send_success_send_timestamp)

if __name__ == "__main__":
    main()
