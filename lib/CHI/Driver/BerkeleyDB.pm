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

has 'db'       => ( is => 'ro', init_arg => undef, lazy_build => 1 );
has 'filename' => ( is => 'ro', init_arg => undef, lazy_build => 1 );
has 'mgr'      => ( is => 'ro', init_arg => undef, lazy_build => 1 );
has 'root_dir' => ( is => 'ro', required => 1 );

__PACKAGE__->meta->make_immutable();

sub BUILD {
    my ( $self, $params ) = @_;

    $self->{mgr_params} = $self->non_common_constructor_params($params);
}

sub _build_mgr {
    my $self = shift;
    return BerkeleyDB::Manager->new(
        home   => $self->root_dir,
        create => 1,
        %{ $self->{mgr_params} },
    );
}

sub _build_filename {
    my $self = shift;
    return $self->escape_for_filename( $self->namespace ) . ".db";
}

sub _build_db {
    my $self = shift;

    my $filename = $self->filename;
    for (1..3) {
        if (my $db = eval { $self->mgr->open_db( file => $filename ) }) {
            return $db;
        }
    }
    die sprintf("cannot open '%s/%s': %s %s", $self->root_dir, $filename, $!, $@);
}

sub fetch {
    my ( $self, $key ) = @_;

    my $data;
    return ( $self->db->db_get( $key, $data ) == 0 ) ? $data : undef;
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

    delete( $self->{db} );
    delete( $self->{mgr} );
    unlink( $self->root_dir . "/" . $self->filename );
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

    my @contents = read_dir( $self->root_dir );
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

    use CHI;

    my $cache = CHI->new(
        driver     => 'BerkeleyDB',
        root_dir   => '/path/to/cache/root'
    );

=head1 DESCRIPTION

This cache driver uses Berkeley DB files to store data. Each namespace is stored in its own db file.

=head1 CONSTRUCTOR OPTIONS

=over

=item root_dir

Path to the directory that will contain the database files, also known as the
BerkeleyDB "Home".

=back

Any other constructor options L<not recognized by CHI|CHI/constructor> are
passed along to L<BerkeleyDB::Manager-E<gt>new>. For example, you can pass
I<db_class> to change from the default BerkeleyDB::Hash.

=head1 AUTHOR

Jonathan Swartz

=head1 SEE ALSO

L<CHI>, L<BerkeleyDB>, L<BerkeleyDB::Manager>

=head1 COPYRIGHT & LICENSE

Copyright (C) 2007 Jonathan Swartz.

CHI::Driver::BerkeleyDB is provided "as is" and without any express or implied
warranties, including, without limitation, the implied warranties of
merchantibility and fitness for a particular purpose.

This program is free software; you can redistribute it and/or modify it under
the same terms as Perl itself.

=cut
