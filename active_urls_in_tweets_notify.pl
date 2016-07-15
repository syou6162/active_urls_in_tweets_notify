#!/usr/bin/env perl
use strict;
use warnings;
use Search::Elasticsearch;
use utf8;
use Encode;
use URI::Find;
use WWW::Mechanize;

my $e = Search::Elasticsearch->new(
    nodes => [ 'localhost:9200', ]
);

my $hour = 2;

my $results = $e->search(
    index => 'twitter_public_timeline',
    type => "tweet",
    body  => {
        query => {
            query_string => {
                query => "(http OR https) AND (syou6162 OR yasuhisay)",
                analyze_wildcard => 1,
            },
        },
        filter => {
            bool => {
                must => [
                    {
                        range => {
                            time => {
                                gte => "now-" . $hour . "H/H",
                                lte => "now",
                                time_zone => "+09:00",
                            },
                        },
                    },
                ],
            },
        },
        size => 3000,
        sort => { time => "desc"}
    }
);

sub extract_urls {
    my $text = shift;
    my $urls = [];
    my $finder = URI::Find->new( sub{ my($uri, $orig_uri) = @_; push @$urls, $orig_uri; });
    $finder->find(\$text);
    return $urls;
}

my $count_by_url = {};
my $users_by_url = {};
my $count_by_url_and_user = {};

foreach my $tweet (@{$results->{hits}->{hits}}) {
    $tweet = $tweet->{_source};
    my $urls = extract_urls $tweet->{text};
    for my $url (@$urls) {
        my $url_and_user = $url . "_" . $tweet->{user};
        $count_by_url_and_user->{$url_and_user}++;
        next if $count_by_url_and_user->{$url_and_user} > 1;
        $count_by_url->{$url}++;
        push @{$users_by_url->{$url}}, $tweet->{user};
    }
}

my $mech = WWW::Mechanize->new;

print encode_utf8 "ここ" . $hour . "時間で話題になったURL一覧です:bow:\n";

foreach ( sort { $count_by_url->{$a} <=> $count_by_url->{$b} } keys $count_by_url ) {
    my $url = $_;
    next if $url eq 'https://t.co';
    next unless $count_by_url->{$url} > 1;
    eval { $mech->get($url); };
    next unless $mech->status == 200;
    my $users = [map { "@" . $_ } @{$users_by_url->{$url}}];
    print encode_utf8 $mech->title . " => (" . join(", ",  @$users) . ")\n";
    print $url, "\n";
    print "\n";
}
