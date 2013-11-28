#!/usr/bin/perl
#
# combine-txt.pl <level> <layer-dir> <img-format> <tile-size-px> <tile-size-u>
#                [<fn> <fe>]
#
# combine texture tiles for level l into texture tiles for level l-1
#
# input:
# <level>: current level
# <layer-dir>: root dir of layer
# <img-format>: image file suffix (i.e. png, tiff, jpg)
# <tile-size-px>: input (and output) tile size in pixels
# <tile-size-u>: input tile size in input SRS units
# <fn>, <fe> false northing and easting.
# 
# input tiles are expected to be named <l>_<k>_<j>.<img-format> and have a
# size of 512x512, arranged in the tile structure produced by tile-images.pl
# script.
#
# output:
# tiles for level <level-1> in the root dir
#
# requirements:
# gdal commandline tools

# params: level, ix, iy, root, sfx 
sub get_src_fname {
	my $l = $_[0];
	my $ix = $_[1];
	my $iy = $_[2];
	my $root = $_[3];
	my $sfx = $_[4];
	
#	<root>/
#	  <level>/
#	    <y-index div 1000>/
#	      <y-index>/
#	        <x-index div 1000>/
#	          <level>_<y-index>_<x-index>.png

	my $fname = $root . "/" . $l . "/" . int($iy / 1000) .
		"/" . $iy . "/" . int($ix / 1000) . "/" . $l . "_" . $iy . "_" . $ix . "." . $sfx;
	
	return $fname;	
}

sub check_and_mkdir {
	my $fname = $_[0];
	
	if ( -e $fname ) {
		if ( ! -d $fname ) {
			die "$fname is not a directory!\n";
		}
	}
	else {
		mkdir $fname; 
	}
}

# params: level, ix, iy, root, suffix 
sub get_dst_fname_and_mkdirs {
	my $l = $_[0];
	my $ix = $_[1];
	my $iy = $_[2];
	my $root = $_[3];
	my $sfx = $_[4];
	
#	<root>/
#	  <level>/
#	    <y-index div 1000>/
#	      <y-index>/
#	        <x-index div 1000>/
#	          <level>_<y-index>_<x-index>.png

	my $fname = $root;
	$fname = $fname . "/" . $l;
	check_and_mkdir($fname);
	$fname = $fname . "/" . int($iy / 1000);
	check_and_mkdir($fname);
	$fname = $fname . "/" . $iy;
	check_and_mkdir($fname);
	$fname = $fname . "/" . int($ix / 1000);
	check_and_mkdir($fname);
	$fname = $fname . "/" . $l . "_" . $iy . "_" . $ix . "." . $sfx;
	
	return $fname;
}

# recursively checks all dirs in the given dir, then all tiles in
# the given dir, and returns an array of four floats, stating the
# tile bbox.
# input:  path, level
# output: (minx, miny, maxx, maxy)
sub get_dir_bbox {
	my $cd = $_[0];
	my $level = $_[1];
	my ($minx, $miny, $maxx, $maxy);
	my ($sminx, $sminy, $smaxx, $smaxy);
	my ($x, $y);
	my ($f, $h);

	$minx = 10000000; 
	$miny = 10000000; 
	$maxx = -10000000; 
	$maxy = -10000000; 

	# print "Opening dir: $cd\n";
	
	opendir $h, $cd || die "Unable to list directory: $cd\n";
	while(defined($f = readdir $h)) {
		# print "File $f\n";
		if($f eq "." || $f eq "..") {
			# ignore current and parent dirs
			next;
		}
		elsif( -d "$cd/$f" ) {
			# entry is a dir: recursively descend into it
			($sminx, $sminy, $smaxx, $smaxy) = get_dir_bbox("$cd/$f", $level);
			# print "Dir $f: extents ($sminx, $sminy, $smaxx, $smaxy)\n";
			if($sminx < $minx) {
				$minx = $sminx;
			}
			if($sminy < $miny) {
				$miny = $sminy;
			}
			if($smaxx > $maxx) {
				$maxx = $smaxx;
			}
			if($smaxy > $maxy) {
				$maxy = $smaxy;
			}
		}
		elsif( -f "$cd/$f" ) {
			# plain file
			($y, $x) = ($f =~ /_([0-9]+)_([0-9]+)\..*$/);
			# print "Tile $f: LL ($x, $y)\n";
			if(defined($x) && defined($y)) {
				if($x < $minx) {
					$minx = $x;
				}
				if($x > $maxx) {
					$maxx = $x;
				}
				if($y < $miny) {
					$miny = $y;
				}
				if($y > $maxy) {
					$maxy = $y;
				}
			}
		}
	} 	
	close $h;
	
	return ($minx, $miny, $maxx, $maxy);	
}

my $usage = "combine-txt.pl <level> <layer-dir> <img-format> " .
	"<tile-size-px> <tiles-size-u> [<fn> <fe> [<my-rank> <num-procs>]] ";

if(scalar @ARGV != 5 && scalar @ARGV != 7 && scalar @ARGV != 9) {
	print $usage . "\n";
	exit 42;
}

my $level = $ARGV[0];
my $root_dir = $ARGV[1];
my $img_fmt = $ARGV[2];
my $tile_size = $ARGV[3];
my $tile_size_u = $ARGV[4];
my ($fn, $fe) = (0, 0);
if(scalar @ARGV >= 7) {
	$fn = $ARGV[5];
	$fe = $ARGV[6];
}

my $rank = $ARGV[7];
my $num_procs = $ARGV[8];

my ($minx, $miny, $maxx, $maxy);

# first, recursively walk the layer dirs and check all tile files in order to
# discover extents covered by tiles

my $f;
my ($l, $j, $k);

($minx, $miny, $maxx, $maxy) = get_dir_bbox
	($root_dir . "/" . $layer . "/" . $level, $level);

# correct UR point
$maxy++;
$maxx++;

print "Tile extents: ($minx, $miny) -> ($maxx, $maxy)\n";

# correct extents: we should begin and end with even indices; yes, end even
# too, as the last combined tile will be made from tiles ($maxx,$maxy),
# ($maxx+1,$maxy), ($maxx,$max_y+1) and ($maxx+1,$maxy+1).
if($minx % 2 != 0) {
	$minx--;
}
if($miny % 2 != 0) {
	$miny--;
}
if($maxx % 2 != 0) {
	$maxx--;
}
if($maxy % 2 != 0) {
	$maxy--;
}

print "Corrected extents: ($minx, $miny) -> ($maxx, $maxy)\n";

my $cmd;
my ($ji, $ki);
my ($sfname, $dfname, $sfnames);
my $anypic;
my $dry_run = 0;
my ($texmin, $texmax, $teymin, $teymax);

# now, we're about to start doing hard work...
for($j = $minx; $j <= $maxx; $j += 2) {
	for($k = $miny; $k <= $maxy; $k += 2) {
		if (($j + $k) % $num_procs == $rank) {
			$anypic = 0;
			# get source images
			$sfnames = "";
			for($ji = 0; $ji <= 1; $ji++) {
				for($ki = 0; $ki <= 1; $ki++) {
					$sfname = get_src_fname($level, $j + $ji, $k + $ki, $root_dir, $img_fmt);
					print "Src fname: $sfname\n";
					if( ! -s $sfname ) {
						# tile does not exist; that's ok with us.
						next;
					}
					$anypic++;
					$sfnames = $sfnames . " " . $sfname;
				}
			}

			if($anypic == 0) {
				# we don't care about empty pics
				next;
			}

			$dfname = get_dst_fname_and_mkdirs($level - 1, $j/2, $k/2,
											   $root_dir, $img_fmt);
			$texmin = $j*$tile_size_u + $fe;
			$texmax = ($j + 2)*$tile_size_u + $fe;
			$teymin = $k*$tile_size_u + $fn;
			$teymax = ($k + 2)*$tile_size_u + $fn;

			$cmd = "gdalwarp -r lanczos -ts $tile_size $tile_size -te $texmin $teymin $texmax $teymax $sfnames $dfname";

			print "Executing: $cmd\n";
			if(!$dry_run) {
				system $cmd || die "trying";
			}
		}
	}
}
