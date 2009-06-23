package CHI::Driver::BerkeleyDB;
use 5.006;
use BerkeleyDB::Manager;
use CHI::Util qw(read_dir);
use CHI::Driver::BerkeleyDB::Util;
use Moose;
use strict;
use warnings;

extends 'CHI::Driver';

our $VERSION = '0.01';

has 'db'   => ( is => 'ro', init_arg => undef, lazy_build => 1 );
has 'file' => ( is => 'ro', init_arg => undef, lazy_build => 1 );
has 'mgr'  => ( is => 'ro', init_arg => undef, lazy_build => 1 );
has 'root_dir' => ( is => 'ro', required => 1 );

__PACKAGE__->meta->make_immutable();

sub _build_mgr {
    my $self = shift;
    return BerkeleyDB::Manager->new(
        home   => $self->root_dir,
        create => 1
    );
}

sub _build_file {
    my $self = shift;
    return $self->escape_for_filename( $self->namespace ) . ".db";
}

sub _build_db {
    my $self = shift;
    return $self->mgr->open_db( file => $self->file );
}

sub fetch {
    my ( $self, $key ) = @_;

    if ( $self->db->db_get( $key, my $data ) == 0 ) {
        return $data;
    }
    else {
        return undef;
    }
}

sub store {
    my ( $self, $key, $data ) = @_;

    $self->db->db_put( $key, $data ) == 0
      or die $BerkeleyDB::Error;
}

sub remove {
    my ( $self, $key ) = @_;

    $self->db->db_del($key) == 0
      or die $BerkeleyDB::Error;
}

sub clear {
    my ($self) = @_;

    undef $self->{db};
    undef $self->{mgr};
    unlink( $self->root_dir . "/" . $self->file );
    $self->{mgr} = $self->_build_mgr;
    $self->{db}  = $self->_build_db;
}

sub get_keys {
    my ($self) = @_;

    my @keys;
    my $cursor = $self->db->db_cursor();
    my ( $key, $value ) = ( "", "" );
    while ( $cursor->c_get( $key, $value, BerkeleyDB::DB_NEXT() ) == 0 ) {
        push( @keys, $key );
    }
    return @keys;
}

sub get_namespaces {
    my ($self) = @_;

    my $root_dir = $self->root_dir;
    my @contents = read_dir($root_dir);
    my @namespaces =
      map { $self->unescape_for_filename( substr( $_, 0, -3 ) ) }
      grep { /\.db$/ } @contents;
    return @namespaces;
}

1;

__END__

=pod

=head1 NAME

CHI::Driver::BerkeleyDB -- Using BerkeleyDB for cache

=head1 SYNOPSIS

    use CHI::Driver::BerkeleyDB;

=head1 DESCRIPTION

CHI::Driver::BerkeleyDB provides

=head1 AUTHOR

Jonathan Swartz

=head1 SEE ALSO

L<CHI>

=head1 COPYRIGHT & LICENSE

Copyright (C) 2007 Jonathan Swartz.

CHI::Driver::BerkeleyDB is provided "as is" and without any express or implied
warranties, including, without limitation, the implied warranties of
merchantibility and fitness for a particular purpose.

This program is free software; you can redistribute it and/or modify it under
the same terms as Per

l it

self.

=cut
