# Jbackup

A backup system based on the format of [zbackup](https://github.com/zbackup/zbackup/) 

# Implemented zbackup features
 * Deduplication
 * Backup and restore
 * Compression based on lzma
 * reading settings from the zbackup store
 
# Currently not implemented zbackup features
 * lzo compression
 * encrypted backups
 * import, export, gc, nbd, inspect
 * saving config options into the zbackup store
 
# Exclusive features
 * Bundles can be safeguarded with reed-solomon code against file defects (not missing files but flipped bits or unreadable disk sectors)
 * restore generate a restore plan to optimize restoring speed and stop cache trashing

# How to build
 mvn package

# How to run


# TODO
 * add missing zbackup features
 * add function to recreate index files from bundles
 * refactor the main function
 * compress bundles in blocks
 * check if we should use direct access
 * add more diagnostics to get info about the compression / deduplication process
 * ...
