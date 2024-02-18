import asyncio

from .component import AnalyzerComponent

async def main():
    analyzer_component = AnalyzerComponent()
    analyzer_event_handler = analyzer_component.disaster_analyzer()
    return await analyzer_event_handler.run()

if __name__ == "__main__":
    asyncio.run(main())
