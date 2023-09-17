# WasteLand

## WasteWeb

WasteWeb is a subproject to add a Web UI for WasteLand.

We use React to build our UI and we also write some code to support some HTTP APIs.

### How to run?

At first you should to build `dist` folder:

```shell
$ pwd
<the-path-of-project>/waste_web/frontend_ui
$ pnpm i
$ pnpm run build
```

Then what you should do is just run the server:

```shell
$ pwd
<the-path-of-project>/waste_web/frontend_ui
$ cargo run
```

## Bench test

### WasteIsland

You should use `./benchmark/src/picture_cache/downloader.sh` to download pictures for test. If it is done, you can just run command below:

```shell
$ cargo bench
```