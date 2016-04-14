#include "mergeruns.h"

#include "logarchiver.h"

#define BOOST_FILESYSTEM_NO_DEPRECATED
#include <boost/filesystem.hpp>
namespace fs = boost::filesystem;

// CS TODO: LA metadata -- should be serialized on run files
const size_t BLOCK_SIZE = 1048576;
const size_t BUCKET_SIZE = 128;

void MergeRuns::setupOptions()
{
    options.add_options()
        ("indir", po::value<string>(&indir)->required(),
            "Directory containing the runs to be merged")
        ("outdir", po::value<string>(&outdir)->default_value(""),
            "Directory where the merged runs will be stored (empty for same as indir)")
        ("minRunSize", po::value<size_t>(&minRunSize)->default_value(0),
            "Minimum size of a merged run (0 for any)")
        ("maxRunSize", po::value<size_t>(&maxRunSize)->default_value(0),
            "Maximum size of a merged run (0 for any)")
        ("fanin", po::value<size_t>(&fanin)->required(),
            "Merge fan-in (required, larger than 1)")
    ;
}

void MergeRuns::run()
{
    if (fanin <= 1) {
        throw runtime_error("Invalid merge fan-in (must be > 1)");
    }

    LogArchiver::ArchiveDirectory* in =
        new LogArchiver::ArchiveDirectory(indir, BLOCK_SIZE, BUCKET_SIZE);

    LogArchiver::ArchiveDirectory* out = in;
    if (!outdir.empty() && outdir != indir) {
        // if directory does not exist, create it
        fs::path fspath(outdir);
        if (!fs::exists(fspath)) {
            fs::create_directories(fspath);
        }
        else {
            if (!fs::is_directory(fspath)) {
                throw runtime_error("Provided path is not a directory!");
            }
        }

        out = new LogArchiver::ArchiveDirectory(outdir, BLOCK_SIZE, BUCKET_SIZE);
    }

    LogArchiver::MergerDaemon merge(in, out);
    W_COERCE(merge.runSync(fanin, minRunSize, maxRunSize));
}
