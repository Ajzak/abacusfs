#!/usr/bin/perl
#
# prepares tiles in an arbitrary SRS from a set of geo-referenced input images
# in another arbitrary SRS
# 
# after numerous input and output SRS and image specific variants, this is the
# culmination of jaKa's geoscripting capabilities, catering to arbitrary SRSes
# and arbitrary input formats, as long as the input images are geotagged 
# 
# requirements:
# GDAL command line tools
# (apt-get install gdal-bin; version 1.6.x was used for development,
# 1.5.x might work, but then again, it might not ...)
# Geo::Proj4 perl module 
# Algorithm::QuadTree perl module
# (use cpan install Geo::Proj4 and cpan install Algorithm::QuadTree to get them)
#
# usage:
#   tile-images.pl <src-dir> <target-dir> <tile-size-u> <tile-size-px> <level>
#                  [<fn> <fe> [<in-srs> <out-srs> [<my-rank> <num-procs>]]] 
#   <src-dir>: directory with input images; all files therein will be considered
#              input images.
#   <target-dir>: directory where output tiles will be put; see below for subdir
#                 hierarchy that will be created
#   <tile-size-u>: tile size in the native units of the target SRS (degrees for
#                  WGS84, metres for Gauss-Krueger etc.)
#   <tile-size-px>: tile size in pixels
#   <level>: the zoom level of tiles (used only in filename)
#   <fn>, <fe>: false northing and easting of tile indices; when (0,0) of index
#               reference system differs from (0,0) of the target SRS,
#   <in-srs>: input SRS (any proj4-friendly string)
#   <out-srs>: output SRS (any proj4-friendly string)
#	<my-rank>, <num-procs>: to make this part of a parallel job.
#				Only every num-procs-th tile will be created.
#
# tile coordinates are given in the tile reference system, with coordinates
# increasing by one towards N and E (an example of this is the WWJ tile
# reference system). also, negative coordinates are undefined in tile reference
# system. (thus, only tiles north and east of (0,0) in trs will be created)
#
# tile directory hierarchy looks like this:
#	  <target-dir>/
#       <level>/
#         <y-index div 1000>/
#           <y-index>/
#             <x-index div 1000>/
#               <level>_<y-index>_<x-index>.png
#
# various useful examples:
# 1. <tile-size-u>:
#   - WGS84: 36.0/(2.0**<level>)
#   - Gauss-Krueger tiles: 1000*(2**(12 - <level>)/512
# 2. SRS IDs:
#   - WGS84: "+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs"
#   - Gauss-Krueger Slo: "+proj=tmerc +lat_0=0 +lon_0=15 +k=0.999900
#                         +x_0=500000 +y_0=-5000000 +ellps=bessel
#                         +towgs84=668,-205,472 +units=m +no_defs"
#   - WGS84 / UTM Zone 33N: "+proj=utm +zone=33 +ellps=WGS84 +datum=WGS84
#                             +units=m +no_defs"
# 3. cutting of 12th level of ortophoto tiles from slo-ortophoto images:
#   tile-images.pl /path/to/ortophotos /tmp/ortophoto 0.008789063 512 12
#                  -90 -180 '+proj=tmerc +lat_0=0 +lon_0=15 +k=0.999900
#                   +x_0=500000 +y_0=-5000000 +ellps=bessel
#                   +towgs84=668,-205,472 +units=m +no_defs'
#                  '+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs'
#

use POSIX qw(ceil floor);
use Geo::Proj4;
use Algorithm::QuadTree;

my $usage = "tile-images.pl <src-dir> <target-dir> <tile-size-u> <tile-size-px> <level> [<fn> <fe> [<in-srs> <out-srs> [<my-rank> <num-procs>]]]";

my ($i, $j, $k);

if(scalar @ARGV != 5 && scalar @ARGV != 7 && scalar @ARGV != 9 && scalar @ARGV != 11) {
	print $usage . "\n";
	exit 42;
}

my $src_dir = $ARGV[0];
my $tgt_dir = $ARGV[1];
my $t_size_u = $ARGV[2];    # was $tile_delta
my $t_size_px = $ARGV[3];
my $t_level = $ARGV[4];
my $t_idx_n = 0;
my $t_idx_e = 0;
my $in_srs = "";
my $out_srs = "";
my $reproject = 0;
if(scalar @ARGV >= 7) {
	$t_idx_n = $ARGV[5];	
	$t_idx_e = $ARGV[6];	
}
if(scalar @ARGV >= 9) {
	$in_srs = $ARGV[7];	
	$out_srs = $ARGV[8];
	$reproject = 1;
}

my $rank = $ARGV[9];
my $num_procs = $ARGV[10];

print "Source files in $src_dir\n";
print "Target files in $tgt_dir\n";
print "Tile size: $t_size_u units, $t_size_px pixels\n";
print "Tile index false northing and easting: ($t_idx_n,$t_idx_e)\n";
if((length $in_srs > 0) && (length $out_srs > 0)) {
	print "Input SRS: $in_srs\n";
	print "Output SRS: $out_srs\n";
}

my ($proj_out, $proj_in);

# coordinate reprojection
if($reproject) {
	$proj_in = Geo::Proj4->new($in_srs);
	$proj_out = Geo::Proj4->new($out_srs);
}

# safe mkdir: will bomb out if the path exists and is not a dir
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

# creates path to tile file, creates all dirs on the path,
# and returns the path
sub get_dst_fname_and_mkdirs {
	my $l = $_[0];
	my $ix = $_[1];
	my $iy = $_[2];
	my $root = $_[3];
	
	my $fname = $root;
	check_and_mkdir($fname);
	$fname = $fname . "/" . $l;
	check_and_mkdir($fname);
	$fname = $fname . "/" . int($iy / 1000);
	check_and_mkdir($fname);
	$fname = $fname . "/" . $iy;
	check_and_mkdir($fname);
	$fname = $fname . "/" . int($ix / 1000);
	check_and_mkdir($fname);
	$fname = $fname . "/" . $l . "_" . $iy . "_" . $ix . ".tif";
	
	return $fname;
}

# reprojects coordinates from input to output SRS
sub reproject_in_to_out {
	my $p = [$_[0], $_[1]];
	my $pp = $proj_in->transform($proj_out, $p);
	return ($$pp[0], $$pp[1]);
}

# reprojects coordinates from output to input SRS
sub reproject_out_to_in {
	my $p = [$_[0], $_[1]];
	my $pp = $proj_out->transform($proj_in, $p);
	return ($$pp[0], $$pp[1]);
}

# collects geo-referencing info on source files, stores it in an array of hashes
# each hash contains the following keys:
# - name: path to source file name
# - ll: lower-left corner of the image
# - ur: upper-right corner of the image
# - origin: image origin
# - pixelsize: size of the image in pixels
# - size: size of the image in input SRS units
#  (values for all keys are 2-element arrays (x,y);
#   ll, ur, and origin are in input SRS units)
sub examine_source_files {
	my $cd = $_[0];
	my $aref = $_[1];
	my $line;
	my $bands;
	my $tmpb;

	opendir $h, $cd || die "Unable to list directory: $cd\n";
	while(defined($f = readdir $h)) {
		# print "File $f\n";
		if($f eq "." || $f eq "..") {
			# ignore current and parent dirs
			next;
		}
		elsif( ( -f "$cd/$f" ) &&
			   (( $f =~ /\.tif$/ ) || ( $f =~ /\.TIF$/ )) ) {
			my %info;
			my ($x, $y);
			my (@ll, @ur, @ps, @or, @sz);
			open F, "gdalinfo $cd/$f|";
			$info{'name'} = "$cd/$f";
			$bands = 0;
			while($line = <F>) {
				chomp $line;
				if(($x, $y) = ($line =~ /^Lower Left\s*\(\s*(-?[0-9.]+),\s*(-?[0-9.]+)\)/)) {
					@ll = ();
					push @ll, $x;
					push @ll, $y;
					$info{'ll'} = \@ll;
				} 
				elsif(($x, $y) = ($line =~ /^Upper Right\s*\(\s*(-?[0-9.]+),\s*(-?[0-9.]+)\)/)) {
					@ur = ();
					push @ur, $x;
					push @ur, $y;
					$info{'ur'} = \@ur;
				} 
				elsif(($x, $y) = ($line =~ /^Pixel Size = \(\s*(-?[0-9.]+),\s*(-?[0-9.]+)\)/)) {
					@ps = ();
					push @ps, $x;
					push @ps, $y;
					$info{'pixelsize'} = \@ps;
				} 
				elsif(($x, $y) = ($line =~ /^Origin = \(\s*(-?[0-9.]+),\s*(-?[0-9.]+)\)/)) {
					@or = ();
					push @or, $x;
					push @or, $y;
					$info{'origin'} = \@or;
				} 
				elsif(($x, $y) = ($line =~ /^Size is\s*(-?[0-9.]+),\s*(-?[0-9.]+)/)) {
					@sz = ();
					push @sz, $x;
					push @sz, $y;
					$info{'size'} = \@sz;
				} 
				elsif(($tmpb) = ($line =~ /^Band ([0-9]+)/)) {
				    if($tmpb > $bands) {
						$bands = $tmpb;
				    }
				} 
			}
			$info{'bands'} = $bands;
			close F;
			push @$aref, \%info;
		}
	} 	
	close $h;
}

sub check_band_count {
	my $aref = $_[0];
	my $info;
	my $bands = 0;
	my $tmpb;

	foreach $info(@$aref) {
		$tmpb = $$info{'bands'};
		if($bands == 0) {
			$bands = $tmpb;
		}
		else {
			if($bands != $tmpb) {
				print "Not all images have the same number of bands.\n";
				print "This is usually due to some images having the alpha " .
					"channel and some not.\n";
				print "Convert all images to the same number of bands " .
					"*before* tiling.\n";
				die;
			}
		}
	}

	return $bands;
}

# dumps source image info collected by examine_source_files()
sub dump_image_list {
	my $aref = $_[0];
	my $info;
	foreach $info(@$aref) {
		print "Info on " . $$info{'name'} . ":\n";
		print "  ll         (" . $$info{'ll'}[0] . "," . $$info{'ll'}[1] . ")\n";  
		print "  ur         (" . $$info{'ur'}[0] . "," . $$info{'ur'}[1] . ")\n";  
		print "  pixel size (" . $$info{'pixelsize'}[0] . "," . $$info{'pixelsize'}[1] . ")\n";
		print "  origin     (" . $$info{'origin'}[0] . "," . $$info{'origin'}[1] . ")\n"; 
		print "  size       (" . $$info{'size'}[0] . "," . $$info{'size'}[1] . ")\n";
		print "  bands      " . $$info{'bands'} . "\n";
	} 
}

# returns bounding box of all source images
sub get_bbox {
	my ($llx, $lly, $urx, $ury) = (1e20, 1e20, -1e20, -1e20);
	my $aref = $_[0];
	my $info;
	
	foreach $info(@$aref) {
		if($llx > $$info{'ll'}[0]) {
			$llx = $$info{'ll'}[0];
		}		
		if($lly > $$info{'ll'}[1]) {
			$lly = $$info{'ll'}[1];
		}		
		if($urx < $$info{'ur'}[0]) {
			$urx = $$info{'ur'}[0];
		}		
		if($ury < $$info{'ur'}[1]) {
			$ury = $$info{'ur'}[1];
		}		
	}
	
	return ($llx, $lly, $urx, $ury);
}

# inserts input images from the array into a quad tree
sub fill_quad_tree {
	my $qtree = $_[0];
	my $aref = $_[1];
	my $info;
	my ($x0, $y0, $x1, $y1);

	foreach $info(@$aref) {
		if($reproject) {
			($x0, $y0) = reproject_in_to_out($$info{'ll'}[0], $$info{'ll'}[1]);
			($x1, $y1) = reproject_in_to_out($$info{'ur'}[0], $$info{'ur'}[1]);
		}
		else {
			($x0, $y0) = ($$info{'ll'}[0], $$info{'ll'}[1]);
			($x1, $y1) = ($$info{'ur'}[0], $$info{'ur'}[1]);		
		}
		print "Adding to quad-tree: " . $$info{'name'} . "@($x0, $y0, $x1, $y1)\n";
		$qtree->add($$info{'name'}, $x0, $y0, $x1, $y1);		
	} 
}

sub concat_fnames {
	my $aref = $_[0];
	my $fname;
	my $rv = "";
	foreach $fname(@$aref) {
		$rv = $rv . " " . $fname;
	} 
	return $rv;
}

my @source_image_list;

# collect info on all source files

print "Analysing input files: this may take a while ...\n";
examine_source_files($src_dir, \@source_image_list);

dump_image_list(\@source_image_list);

print "Checking band count ...\n";
# check band count of all images
my $bands = check_band_count(\@source_image_list);

print "Computing bounding boxes...\n";
# compute bounding box (in input SRS units)
my ($illx, $illy, $iurx, $iury) = get_bbox(\@source_image_list);
my ($ollx, $olly, $ourx, $oury);
# compute bounding box (in output SRS units)
if($reproject) {
	# reprojection required (i.e. input SRS != output SRS)
	($ollx, $olly) = reproject_in_to_out($illx, $illy);
	($ourx, $oury) = reproject_in_to_out($iurx, $iury);
}
else {
	# input and output SRS are the same
	($ollx, $olly, $ourx, $oury) = ($illx, $illy, $iurx, $iury);
}

print "Bounding box (input SRS): ($illx,$illy) -> ($iurx,$iury)\n";
print "Bounding box (output SRS): ($ollx,$olly) -> ($ourx,$oury)\n";

# compute bounding box in tile index reference system
my ($tllx, $tlly, $turx, $tury) = (0, 0, 0, 0);

print "Computing bounding box (tile index reference system). This may, again, take a while ...\n";
for($tllx = 0; ($tllx + 1)*$t_size_u < $ollx - $t_idx_e; $tllx++) {}
for($tlly = 0; ($tlly + 1)*$t_size_u < $olly - $t_idx_n; $tlly++) {}
for($turx = $tllx; ($turx + 1)*$t_size_u < $ourx - $t_idx_e; $turx++) {}
for($tury = $tlly; ($tury + 1)*$t_size_u < $oury - $t_idx_n; $tury++) {}

print "Bounding box (tile index reference system): ($tllx,$tlly) -> ($turx,$tury)\n";

# try experimenting with tree depth if 10 levels proves too slow ...
my $qtree = Algorithm::QuadTree->new(-xmin => $ollx,
                                     -xmax => $ourx,
                                     -ymin => $olly,
                                     -ymax => $oury,
                                     -depth => 10);

print "Filling quad-tree ...\n";
fill_quad_tree($qtree, \@source_image_list);

#(13.4569364467462,45.9527763465387) -> (13.5418545908384,46.0348708088821)
#print "Testing ...\n";
#my ($tx0, $ty0, $tx1, $ty1) = (13.46,46,13.48,46.01);
#my $files = $qtree->getEnclosedObjects($tx0, $ty0, $tx1, $ty1);
#foreach $file(@$files) {
#	print "File: $file\n";	
#}

# warping parameters
#my $warp_params = "-r cubicspline -multi";
my $warp_params = "-r lanczos -multi";
# alpha parameters
my $alpha_params = "";
# srs parameters
my $srs_params = "";
# add other params here (like src and dstnodata)
my $other_params = "";
# examples: "-srcnodata '255 255 255' -dstnodata '255 255 255'"

my ($cmd, $te, $dst, $fnames, $fnames_str);
my ($tx, $ty, $ox, $oy);
my $dry_run = 0;

if($reproject) {
	# add source and target SRS parameters
	$srs_params = "-s_srs '$in_srs' -t_srs '$out_srs'"; 
}

if($bands < 4) {
	# add alpha channel to target image as source images dont have one
	$alpha_params = "-dstalpha"
}

my ($totx, $toty);
$totx = $turx - $tllx;
$toty = $tury - $tlly;

for($tx = $tllx; $tx <= $turx; $tx++) {
	for($ty = $tlly; $ty <= $tury; $ty++) {

		if (($tx + $ty) % $num_procs == $rank) {

			($ox, $oy) = ($tx*$t_size_u + $t_idx_e, $ty*$t_size_u + $t_idx_n);
			# note to self: use a slightly bigger bounding box to make sure no
			# required source images are left out. computers do work with a
			# finite precision floating point, you know.
			$fnames = $qtree->getEnclosedObjects($ox - $t_size_u, $oy - $t_size_u,
												 $ox + 2*$t_size_u, $oy + 2*$t_size_u);
			$fnames_str = concat_fnames($fnames);
			if(length $fnames_str == 0) {
				print "No input tiles.\n";
				next;
			}

			$dst = get_dst_fname_and_mkdirs($t_level, $tx, $ty, $tgt_dir);
			if( -e "$dst") {
				printf "Tile exists: $dst\n";
				next;
			}

			$te = ($ox) . " " . ($oy) . " " .
				($ox + $t_size_u) . " " . ($oy + $t_size_u);
			print "Processing tile for $te\n";
			$cmd = "gdalwarp $warp_params $alpha_params $other_params $srs_params -te $te -ts $t_size_px $t_size_px $fnames_str $dst";
			print "Warping file $dst: [$cmd]\n";
			if(!$dry_run) {
				system $cmd || die "trying!";
			}

			print "Done: " . ($ty - $tlly + 1 + 
							  ($tx - $tllx)*($tury - $tlly + 1)) . "/" .
							  ($totx + 1)*($toty + 1) . "\n";
		}
	}
}
