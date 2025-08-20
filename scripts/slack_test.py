from slack_sdk import WebClient

client = WebClient(token='#슬랙토큰')
client.chat_postMessage(channel='#새-채널', text="오")
