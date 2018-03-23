# 0.4.1

* [PR-44](https://github.com/Valassis-Digital-Media/spylon-kernel/pull/44) - Fix Spark 2.2.1 compatibility

# 0.4.0

* [PR-38](https://github.com/maxpoint/spylon-kernel/pull/38) - Capture all stdout/stderr from the py4j JVM

# 0.3.2

* [PR-36](https://github.com/maxpoint/spylon-kernel/pull/35) - Revert to setting stdout/stderr once, fix typos and nits

# 0.3.1

* [PR-34](https://github.com/maxpoint/spylon-kernel/pull/34) - Test on Spark 2.1.1 and Python 3.5, 3.6
* [PR-35](https://github.com/maxpoint/spylon-kernel/pull/35) - Mark stdout / stderr PrintWriters as @transient

# 0.3.0

* [PR-24](https://github.com/maxpoint/spylon-kernel/pull/24) - Let users set `application_name` in `%%init_spark`
* [PR-29](https://github.com/maxpoint/spylon-kernel/pull/29), [PR-30](https://github.com/maxpoint/spylon-kernel/pull/30) - General code cleanup and documentation
* [PR-31](https://github.com/maxpoint/spylon-kernel/pull/31) - Fix lost stdout/stderr from non-main threads
* [PR-31](https://github.com/maxpoint/spylon-kernel/pull/31) - Fix run all halts on comment-only cells
* [PR-33](https://github.com/maxpoint/spylon-kernel/pull/33) - Update README, travis.yml, Makefile for Spark 2.1.1 and Python 3.6

# 0.2.1

* [PR-17](https://github.com/maxpoint/spylon-kernel/pull/17) - Fix anonymous function class defs not found
* [PR-19](https://github.com/maxpoint/spylon-kernel/pull/19) - Fix clipped tracebacks
* Automate developer, maintainer setup

# 0.2.0

* [PR-15](https://github.com/maxpoint/spylon-kernel/pull/15) - Fix to support jedi>=0.10
