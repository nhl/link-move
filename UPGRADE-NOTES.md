_This document contains upgrade notes for LinkMove 3.x and newer. Older versions are documented in
[UPGRADE-NOTES-1-2](./UPGRADE-NOTES-1-to-2.md)._

## Upgrading to 3.0.M1

### Switch to Java 11 [#199](https://github.com/nhl/link-move/issues/199)
Java 11 is the minimal required Java version. If you are still on Java 8, please continue using LinkMove 2.x.

### Switch to Cayenne 4.2 [#200](https://github.com/nhl/link-move/issues/200)
The new version of Cayenne used by LinkMove is 4.2 (upgraded from the ancient 4.0.2). You will need to upgrade
your application Cayenne projects.