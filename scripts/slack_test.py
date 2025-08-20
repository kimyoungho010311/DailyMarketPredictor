from slack_sdk import WebClient

client = WebClient(token='xoxb-9378847727636-9373422316853-QVELvDID3cibyidpTX7z3ETW')
client.chat_postMessage(channel='#새-채널', text="오")
