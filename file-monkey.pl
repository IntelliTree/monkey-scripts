#! /usr/bin/perl

=head1 name

file-monkey.pl - create filesystem activity simulating a samba fileserver workload

=head1 SYNOPSIS

file-monkey.pl [options] # no options required

 Option Summary:
   --help               brief help
   --man                full documentation
                     
   --mkdir X            probability of creating new directory
   --rmdir X            probability of recursively deleting a directory
   --mkfile X           probability of creating a file
   --rmfile X           probability of deleting a file
   --append X           probability of appending an existing file
   
   All probabilities are numbers (0..1), and must add up to less than 1.  Unused
   probability range will be the probability of re-writing a file.
   
   --root ROOT          root directory for file activity.  will be created if missing.
   --chroot             chroot into ROOT.  requires /bin/busybox.static
   --no-chroot          do not chroot into ROOT (but still chdir)
   --max-dirs N         maximum number of directories allowed in ROOT
   --max-disk-usage N   max bytes of files allowed in ROOT
   
   The default is to chroot to /opt/site/file-monkey and work on 10GB of files,
   max 8000 directories.

=cut

use strict;
use warnings;
use Try::Tiny;
use String::Random;
use Getopt::Long;
use Cwd 'realpath';

my $root= '/opt/site/file-monkey';

my $mkdir_chance=   0.02;
my $rmdir_chance=   0.005;
my $mkfile_chance=  0.2;
my $rmfile_chance=  0.07;
my $append_chance=  0.3;
# remaining probability is rewrite a file

my $dir_leaf_limit= 8000;
my $size_limit= 10*1024*1024*1024;
my $chroot= 1;

GetOptions (
	'help|?'           => sub { pod2usage(1); },
	'man'              => sub { pod2usage(-exitval => 1, -verbose => 2); },
	'mkdir=f'          => \$mkdir_chance,
	'rmdir=f'          => \$rmdir_chance,
	'mkfile=f'         => \$mkfile_chance,
	'rmfile=f'         => \$rmfile_chance,
	'append=f'         => \$append_chance,
	'root=s'           => \$root,
	'max-dirs=i'       => \$dir_leaf_limit,
	'max-disk-usage=i' => \$size_limit,
	'chroot!'          => \$chroot,
) or pod2usage();

die "Invalid root \"$root\"\n"
	unless defined $root and length $root and index($root,"'") == -1;

`mkdir -p '$root'`;
$? == 0 or die "mkdir failed";
$root= realpath($root);
die "Cowardly refusing to run in \"$root\" because it isn't 3 directories deep\n"
	unless 2 < scalar(()= $root =~ m|(/)|g);
chdir $root or die "Can't chdir to $root: $!";

if ($chroot) {
	`mkdir -p ./bin`;
	`cp /bin/busybox.static ./bin/`;
	link('./bin/busybox.static', './bin/find');
	link('./bin/busybox.static', './bin/rm');
	symlink('bin', './sbin');
	symlink('.', './usr');
	chroot '.' or warn "Can't chroot: $!";
}

my $str_rand= String::Random->new;
my @subpaths;
my $disk_usage;
my $file_count;

sub refresh_tree_stats {
	@subpaths= ();
	$file_count= 0;
	$disk_usage= 0;
	open my $find, "find . |" or die "Can't run find";
	while (<$find>) {
		chomp;
		lstat;
		if (-d _) {
			push @subpaths, $_ unless $_ eq '.' or $_ =~ /^\.\/bin/;
		}
		elsif (-f _) {
			$file_count++;
			$disk_usage+= -s $_;
		}
	}
	print STDERR "(".@subpaths." dirs, $disk_usage bytes in $file_count files)\n";
}

refresh_tree_stats;
while (1) {
	# Wait 1 to 8 seconds
	sleep int(rand 8);
	try {
		my $choice= rand;
		my $dir= pick_random_dir();
		my $fname= pick_random_file();
		
		# Create a new directory?
		if (($choice-= $mkdir_chance) < 0 or !defined $dir) {
			print "new dir,  ";
			print create_dir()."\n";
		}
		# Remove an entire directory?
		elsif (($choice-= $rmdir_chance) < 0 or @subpaths > $dir_leaf_limit) {
			print "rmdir $dir,  ";
			print rm_dir($dir)."\n";
		}
		# Create a new file?
		elsif (($choice-= $mkfile_chance) < 0 or !defined $fname) {
			print "new file in $dir,  ";
			print create_file($dir)."\n";
		}
		# Remove a file?
		elsif (($choice-= $rmfile_chance) < 0 or $disk_usage > $size_limit) {
			print "rm file $fname,  ";
			print rm_file($fname)."\n";
		}
		# Append a file?
		elsif (($choice-= $append_chance) < 0) {
			print "append file $fname,  ";
			print append_file($fname)."\n";
		}
		# else rewrite file
		else {
			print "rewrite file $fname,  ";
			print rewrite_file($fname)."\n";
		}
	}
	catch {
		chomp($_);
		warn "\n$_";
		refresh_tree_stats();
	}
}

sub pick_random_dir {
	@subpaths? $subpaths[ int rand @subpaths ] : undef;
}

sub pick_random_file {
	# try from 15 random directories before returning undef (indicating abundance of empty directories)
	return undef unless @subpaths;
	for (my $i= 0; $i < 15; $i++) {
		my $d= pick_random_dir();
		my @files= grep { -f $_ } <$d/*>;
		return $files[ int rand @files ]
			if @files;
	}
	return undef;
}

sub create_file {
	my $dir= shift;
	my $fname= $dir . '/' . $str_rand->randregex('\w\w\w\w\w\w\w\w\w\w');
	# don't bother checking to be unique, doesn't really matter if it fails
	open my $fh, ">", $fname
		or die "open($fname): $!";
	print $fh $str_rand->randregex('\w') x 1024
		for 0 .. int rand 1024;
	close $fh or warn "close($fname): $!";
	$file_count++;
	$disk_usage+= -s $fname;
	return $fname;
}

sub append_file {
	my $fname= shift;
	my $s1= -s $fname;
	open my $fh, ">>$fname" or die "$!";
	print $fh $str_rand->randpattern("." x 1024)
		for 0 .. int rand 1024;
	close $fh or warn "close($fname): $!";
	$disk_usage+= -$s1 + -s $fname;
	1;
}

sub rewrite_file {
	my $fname= shift;
	my $s1= -s $fname;
	open my $fh, ">.new-$fname" or die "$!";
	print $fh $str_rand->randpattern("." x 1024)
		for 0 .. $s1/1024;
	close $fh;
	unlink($fname) or warn "unlink($fname): $!";
	rename(".new-$fname", $fname) or warn "rename($fname): $!";
	$disk_usage+= -$s1 + -s $fname;
	1;
}

sub rm_file {
	my $fname= shift;
	my $size= -s $fname;
	unlink($fname) or die "Can't unlink $fname: $!";
	$file_count--;
	$disk_usage-= $size;
	1;
}

sub create_dir {
	my $i= int rand 1+@subpaths;
	my $dir= ($i >= @subpaths)? '.' : $subpaths[$i];
	my $path= $dir . '/' . $str_rand->randregex('\w\w\w\w\w\w\w\w');
	# don't bother checking to be unique, doesn't really matter if it fails
	mkdir $path or die "mkdir($path): $!";
	push @subpaths, $path;
	$path;
}

sub rm_dir {
	my $dir= shift;
	`rm -rf '$dir'`;
	$? == 0 or die "rm $dir failed";
	refresh_tree_stats();
	1;
}
