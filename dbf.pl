#!/usr/bin/perl
use warnings;
use strict;


use Getopt::Long; # Работа с командной строкой и конфигурациями
use Cwd;

use XBase; # Работа с DBF файлами

use Data::Dumper;

use XML::Simple qw(:strict); # Вывод в XML
use JSON; # Вывод в JSON

my %c; # Хеш с настройками

# Получение данных из командной строки
GetOptions(\%c,'help|?|h','dir|d=s','tmp|t=s','method|m=s',
	'output|o=s','quit|q','debug','unzip|u=s','iconv|i');

if (defined $c{'dir'}) {
	$c{'dir'} = Cwd::abs_path($c{'dir'});
} else {
	$c{'dir'} = getcwd();
}

if (defined $c{'tmp'}) {
	$c{'tmp'} = Cwd::abs_path($c{'tmp'});
} else {
	$c{'tmp'} = $c{'dir'}.'/'.$c{'time'};
}

$c{'method'} = 'xml' unless (defined $c{'method'} 
	and ($c{'method'} eq 'xml' or $c{'method'} eq 'json' or 
		$c{'method'} eq 'html' or $c{'method'} eq 'dump'));

if (!defined $c{'output'}) {
	$c{'output'} = $c{'tmp'}.'/'.$c{'method'};
} else {
	$c{'output'} = Cwd::abs_path($c{'output'});
}
$c{'debug'} = 0 unless defined $c{'debug'};
$c{'unzip'} = '/usr/bin/unzip' unless defined $c{'unzip'};
$c{'iconv'} = '/usr/bin/iconv' unless defined $c{'iconv'};
$c{'time'} = time();

die "Can't execute unzip [$c{'unzip'}]\n" unless (-x $c{'unzip'});
die "Can't execute iconv [$c{'iconv'}]\n" unless (-x $c{'iconv'});

if (defined($c{'help'})) {
	print "Usage: $0 opts\n";
	print "-d /path/to/russianpost/files/ .. [$c{'dir'}]\n";
	print "-t /path/to/tmp/folder/ ......... [$c{'tmp'}]\n";
	print "-m output method: xml/json/html . [$c{'method'}]\n";
	print "-o output folder: ............... [$c{'output'}]\n";
	print "-u /path/to/unzip: .............. [$c{'unzip'}]\n";
	print "-i /path/to/iconv: .............. [$c{'iconv'}]\n";
	print "-? this message\n";
	print "-q quit before work\n";
	print "--debug\n";
	print "\n\nmailto:ddsh\@ddsh.ru\n";
	print "github:https://github.com/lariliel/rpopsdb/\n";
	print "RussianPost DOCS: http://info.russianpost.ru/database/ops.html\n";
}
die "Died by --quit|q passed\n" if defined $c{'quit'};

print "Start with output folder [$c{'output'}] at [$c{'time'}]\n"  if $c{'debug'};

# Начало обработки списка файлов
print "Open [$c{'dir'}]\n" if $c{'debug'} ;
die "Can't open [$c{'dir'}]" unless (-d $c{'dir'});

# Создаём папки для работы программы
unless (-d $c{'tmp'}) {
	mkdir $c{'tmp'} || die "Can't mkdir($c{'tmp'})";
}
unless (-d $c{'output'}) {
	mkdir $c{'output'} || die "Can't mkdir($c{'output'})";
}

# Начало работы со списком файлов
while(<$c{'dir'}/*.zip>){ # Извлечение архивов в $c{tmp}
	`$c{'unzip'} $_ -d $c{'tmp'}`;
	print "$c{'unzip'} $_ -d $c{'tmp'}\n";
}

my %count; # Хеш количеств строк.
print "Read INDEX db now... " if $c{'debug'};
my %i; # Хеш индексов, устройство описано здесь - http://info.russianpost.ru/database/ops.html
while (<$c{'tmp'}/PInd*.DBF>) { # Обработка файлов базы почтовый отделений
	my $table = new XBase $_ or die XBase->errstr;
	for (0 .. $table->last_record) {
		# FIXME: Чёртов быдлокод.
		my ($deleted, $Index, $OPSName, $OPSType, $OPSSubm,
			$Region, $Autonom, $Area, $City, $City1, $ActDate, $IndexOld)
                = $table->get_record($_, "Index", "OPSName",
                	"OPSType", "OPSSubm", "Region", "Autonom",
                	"Area", "City", "City1", "ActDate", "IndexOld");
        $i{$Index} = {
        	'Index' => $Index,
        	'OPSName' => $OPSName || " ",
        	'OPSType' => $OPSType || " ",
        	'OPSSubm' => $OPSSubm || " ",
        	'Region' => $Region || " ",
        	'Autonom' => $Autonom || " ",
        	'Area' => $Area || " ",
        	'City' => $City || " ",
        	'City1' => $City1 || " ",
        	'ActDate' => $ActDate || " ",
        	'IndexOld' => $IndexOld || " "
        };
        $i{$Index}{'RESTRICTIONS'} = [];
        $i{$Index}{'RESTRICTIONSN'} = 0;
        $count{'INDEX'}++;
	}
}
print "$count{'INDEX'} lines.\n" if $c{'debug'};

print "Read DLV db now... " if $c{'debug'};
while (<$c{'tmp'}/Dlv*.DBF>) { # добавляем ограничения на пересылку в базу http://info.russianpost.ru/database/dlimits.html
	my $table = new XBase $_ or die XBase->errstr;
	for (0 .. $table->last_record) {
		# FIXME: Чёртов быдлокод.
		my ($deleted, $Index, $OPSName, $ActDate, $PrBegDate,
			$PrEndDate, $DelivType, $DelivPnt, $BaseRate,
			$BaseCoeff, $TransfCnt, $RateZone, $CfActDate,
			$DelivIndex)
                = $table->get_record($_, "Index", "OPSName",
                	"ActDate", "PrBegDate", "PrEndDate",
                	"DelivType", "DelivPnt", "BaseRate",
                	"BaseCoeff", "TransfCnt", "RateZone",
                	"CfActDate", "DelivIndex");

        $i{$Index}{'RESTRICTIONS'}[ $i{$Index}{'RESTRICTIONSN'} ] = {
        	'OPSName' => $OPSName,
        	'ActDate' => $ActDate,
        	'PrBegDate' => $PrBegDate,
        	'PrEndDate' => $PrEndDate,
        	'DelivType' => $DelivType,
        	'DelivPnt' => $DelivPnt,
        	'BaseRate' => $BaseRate,
        	'BaseCoeff' => $BaseCoeff,
        	'TransfCnt' => $TransfCnt,
        	'RateZone' => $RateZone,
        	'CfActDate' => $CfActDate,
        	'DelivIndex' => $DelivIndex,
        };
        $count{'DLV'}++;
        $i{$Index}{'RESTRICTIONSN'}++;
	}
}
print "$count{'DLV'} lines.\n" if $c{'debug'};

print "Read RZ db now... " if $c{'debug'};
while (<$c{'tmp'}/RZ*.DBF>) { # Добавляем тарифные зоны http://info.russianpost.ru/database/tzones.html
	my $table = new XBase $_ or die XBase->errstr;
	for (0 .. $table->last_record) {
		my ($deleted, $Index, $RateZone)
                = $table->get_record($_, "Index", "RateZone");
        $i{$Index}{'RZRateZone'} = $RateZone || " ";
		$count{'RZ'}++;
	}
}
print "$count{'RZ'} lines.\n" if $c{'debug'};

# Переходим к формированию файлов вывода:
my $output = "NONE";
my $fout = $c{'output'}.'/'.$c{'time'}.'.'.$c{'method'};
if ($c{'method'} eq 'xml') {
	$output = XMLout(\%i, KeyAttr => "Index");
}
if ($c{'method'} eq 'json') {
	$output = to_json(\%i);
}
if ($c{'method'} eq 'html') {
	$output = <<"EOT";
	<html>
		<head>
			<title>RussianPostDB buildtime $c{'time'}</title>
		</head>
		<body>
			<p>Based on <a href="http://info.russianpost.ru/database/ops.html">RussianPostDB</a>.</p>
			<p>Contains $count{'INDEX'} OPS.<p>
			<table border="1">
			<tr>
				<td>Index</td>
				<td>OPSName</td>
				<td>OPSType</td>
				<td>OPSSubm</td>
				<td>Region</td>
				<td>Autonom</td>
				<td>Area</td>
				<td>City</td>
				<td>City1</td>
				<td>ActDate</td>
				<td>IndexOld</td>
				<td>RateZone</td>
			</tr>
EOT
	foreach my $index (keys %i) {
		my $OP = $i{$index};
		$output .= <<"EOT";
		<tr id='$index'>
		<td>$index</td>
		<td>$OP->{'OPSName'}</td>
		<td>$OP->{'OPSType'}</td>
		<td>$OP->{'OPSSubm'}</td>
		<td>$OP->{'Region'}</td>
		<td>$OP->{'Autonom'}</td>
		<td>$OP->{'Area'}</td>
		<td>$OP->{'City'}</td>
		<td>$OP->{'City1'}</td>
		<td>$OP->{'ActDate'}</td>
		<td>$OP->{'IndexOld'}</td>
		<td>$OP->{'RZRateZone'}</td>
		</tr>
EOT
	}
	$output .= <<"EOT";
	</table>
	<a href="https://github.com/lariliel/rpopsdb/">project on GitHub</a>.
	</body></html>
EOT
}

if ($c{'method'} eq 'dump') {
	$output = Dumper(%i);
}

open my $fp, '>', $fout.'.tmp' || die "Can't open $fout for write!";

print $fp $output;

`$c{'iconv'} -f CP866 -t UTF-8 $fout.'tmp' > $fout`;
unlink($fout.'.tmp');

print "Done!\n" if $c{'debug'};
