#!/usr/bin/env perl


use strict;
use warnings;
use DBI;
use DBD::Pg;
use Try::Tiny;

use Data::Dumper;

use JSON::XS;
use File::Find;



use utf8;
# some code from
# https://www.perlmonks.org/?node_id=1147720

my $total = 0;
my $current = 1;
my $do_commit = 1;
my @buf;
my $json_xs = JSON::XS->new()->utf8(1);

my $base = 'sql/pgmig/';

# raise messages as json strings
my $proto_ver = q(select set_config('pgmig.assert_proto', '1', true););

 sub handle_error {
  my $message = shift;
  my $h = shift;
  printf "%s: %s\n", $h->state, $h->errstr;
  return "$message";
}

sub handle_message{
  my $message = shift;
  if ($message =~ /^NOTICE:  \{/) {
    $message =~ s/^NOTICE:  //;
    chomp $message;
    my $data = $json_xs->decode($message);

    if ($data->{'code'} eq '01998') {
        $total = $data->{'message'};
        $current = 1;
    } elsif ($data->{'code'} eq '01999') {
        printf "(%d/%d) %-20s: Ok\n", $current++, $total, $data->{'message'};
        @buf = ();
    } elsif ($data->{'code'} eq '02999') {
        printf "(%d/%d) %-20s: Not Ok\ngot: %s\nwant: %s\n", $current++, $total, $data->{'message'}
        , $data->{'data'}{'got'},$data->{'data'}{'want'};
        print @buf;
        (@buf) = ();
        $do_commit = 0;
    } else {
      push @buf, $message;
    }
  } else {
    # save message
  #  push @buf, $message;
    print $message;
  }
}


sub main() {

  my $dbh = DBI->connect('dbi:Pg:', undef, undef,
    {
      AutoCommit => 0,  # enable transactions
      RaiseError => 1,
      HandleError => \&handle_error,
      pg_server_prepare => 0,
    }
  ) or handle_error(DBI->errstr);

  local $SIG{__WARN__} = \&handle_message;

  my @files;
    find(sub {
      if (-f and /\.sql$/) {
        push @files, $_;
      }
    }, $base);
  print "Found:",Dumper(\@files);

  try {
    $dbh->do($proto_ver) or die 'Setup failed';
    #$dbh->do('set client_encoding to utf8') or die 'Setup failed';

    foreach my $f (sort @files) {
      my $file_path = $base."$f";# 'testdata/'.$_;
      printf "Load: %s\n", $file_path;
      open my $fh, '<', $file_path
        or die "Error opening $file_path - $!\n";
      my $cmds;
      { local $/ = undef; $cmds = <$fh>; }
      $dbh->do($cmds) or die 'Error found';
      $do_commit or die 'Test failed';
    }

    $dbh->commit;   # commit the changes if we get this far
    print "Finished\n";
  } catch {
    if (!$do_commit) {
      printf "Rollback ($_)\n";
    } else {
      printf "Transaction aborted (%s:%s) $_\n", $dbh->state, $dbh->errstr; # Try::Tiny copies $@ into $_
    }

    # now rollback to undo the incomplete changes
    # but do it in an eval{} as it may also fail
    eval { $dbh->rollback };
    # add other application on-error-clean-up code here
    return 1;
  };
  return 0;
}

exit(main());
