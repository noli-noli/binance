# Docker Sample 2025
本リポジトリはdocker-compose用のサンプルファイル群である。
学内で利用するために必須であるproxy用設定やGPU設定(Linux・Windows)は予め記述されている。よって、雛形としての利用を想定している。

## discord.py 忘備録

`recoder_source/bot_test.py`に書いた内容のメモ。

### Bot起動時

```python
intents = discord.Intents.default()
intents.message_content = True

bot = commands.Bot(command_prefix="!", intents=intents)

@bot.event
async def on_ready():
    print(f"Login : {bot.user}")
```

`message_content = True`を指定すると、メッセージ本文を取得できる。

### メッセージ受信

```python
@bot.event
async def on_message(message):
    if message.author.bot:
        return
```

Bot自身のメッセージは無視する。

### メッセージから取れる情報

- `message.id` : メッセージID
- `message.channel.id` : チャンネルID
- `message.guild.id` : サーバーID
- `message.author.id` : 送信者のユーザーID
- `message.author.name` : 送信者のユーザー名
- `message.author.display_name` : サーバー上の表示名
- `message.content` : メッセージ本文
- `message.created_at` : 送信日時
- `message.guild.name` : サーバー名
- `message.channel.name` : チャンネル名

### メンション検知

```python
if bot.user in message.mentions:
    await message.channel.send("ﾆﾝｹﾞﾝ...ｺﾛｽ...")
```

Botがメンションされたかを判定できる。

### 埋め込みメッセージ

```python
embed = discord.Embed(
    title="通知",
    description="サーバーが起動しました",
    color=0x00ff88
)

embed.add_field(name="状態1", value="正常1", inline=True)
embed.add_field(name="状態2", value="正常2", inline=True)

await message.channel.send(embed=embed)
```

`discord.Embed`を使うと、通知っぽい見た目のメッセージを送れる。

### 入力中表示

```python
async with message.channel.typing():
    await asyncio.sleep(2)
```

Botが入力中の表示になる。

### コマンド処理

```python
await bot.process_commands(message)
```

`on_message`を書いた場合、コマンド処理を続けるために最後に呼ぶ。
