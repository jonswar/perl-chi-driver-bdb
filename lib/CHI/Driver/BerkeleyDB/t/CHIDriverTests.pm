package CHI::Driver::BerkeleyDB::t::CHIDriverTests;
use strict;
use warnings;
use CHI::Test;
use File::Slurp;
use File::Temp qw(tempdir);
use base qw(CHI::t::Driver);

my ( $root_dir, $root_dir_initialized );

sub testing_driver_class { 'CHI::Driver::BerkeleyDB' }

sub clear_root_dir : Test(setup) {
    $root_dir_initialized = 0;
}

sub new_cache_options {
    my $self = shift;

    # Generate new temp dir for each test method that needs it;
    # previous temp dir gets cleaned up immediately
    #
    if ( !( $root_dir_initialized++ ) ) {
        $root_dir =
          File::Temp->newdir( "chi-driver-berkeleydb-XXXX", TMPDIR => 1 );
    }
    return ( $self->SUPER::new_cache_options(), root_dir => $root_dir );
}

1;
